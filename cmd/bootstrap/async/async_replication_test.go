package async

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"

	database "github.com/percona/percona-server-mysql-operator/cmd/internal/db"
	mysqldb "github.com/percona/percona-server-mysql-operator/pkg/db"
)

// fakePeer is one peer as getTopology sees it. A nil-free fakePeer answers;
// connectErr makes the connection itself fail, statusErr makes the first
// query fail after a successful connect (mysqld up but not serving yet).
type fakePeer struct {
	status     mysqldb.ReplicationStatus
	source     string
	reportHost string
	connectErr error
	statusErr  error
	closed     bool
}

func (p *fakePeer) ReplicationStatus(context.Context) (mysqldb.ReplicationStatus, string, error) {
	if p.statusErr != nil {
		return mysqldb.ReplicationStatusError, "", p.statusErr
	}
	return p.status, p.source, nil
}

func (p *fakePeer) ReportHost(context.Context) (string, error) { return p.reportHost, nil }

func (p *fakePeer) Close() error {
	p.closed = true
	return nil
}

// withPeers routes newPeerDB to the given peers by host for the duration of
// the test. A host absent from the map is a lookup failure, like a pod that
// is not in the headless service yet.
func withPeers(t *testing.T, peers map[string]*fakePeer) {
	t.Helper()
	orig := newPeerDB
	newPeerDB = func(_ context.Context, params database.DBParams) (peerDB, error) {
		p, ok := peers[params.Host]
		if !ok {
			return nil, errors.New("no such host")
		}
		if p.connectErr != nil {
			return nil, p.connectErr
		}
		return p, nil
	}
	t.Cleanup(func() { newPeerDB = orig })
}

func TestGetTopology(t *testing.T) {
	const (
		pod0 = "cluster-mysql-0.cluster-mysql.ns"
		pod1 = "cluster-mysql-1.cluster-mysql.ns"
		pod2 = "cluster-mysql-2.cluster-mysql.ns"
	)
	refused := errors.New("dial tcp: connection refused")

	replicaOf := func(primary, self string) *fakePeer {
		return &fakePeer{status: mysqldb.ReplicationStatusActive, source: primary, reportHost: self}
	}
	standalone := func(self string) *fakePeer {
		return &fakePeer{status: mysqldb.ReplicationStatusNotInitiated, reportHost: self}
	}
	// A replica whose threads are down: mysqld came up but did not resume
	// replication (corrupt relay log after a crash is the usual reason). It
	// reports no source.
	stalled := func(self string) *fakePeer {
		return &fakePeer{status: mysqldb.ReplicationStatusStopped, reportHost: self}
	}

	tests := []struct {
		name         string
		self         string
		peers        map[string]*fakePeer
		wantPrimary  string
		wantReplicas []string
		wantErr      string
	}{
		{
			name: "healthy cluster",
			self: pod2,
			peers: map[string]*fakePeer{
				pod0: standalone(pod0),
				pod1: replicaOf(pod0, pod1),
				pod2: replicaOf(pod0, pod2),
			},
			wantPrimary:  pod0,
			wantReplicas: []string{pod1, pod2},
		},
		{
			// pod-1 is restarting at the same time. Its startup probe is
			// running this very code against us; if we fail on it and it
			// fails on us, neither pod ever becomes ready.
			name: "one peer refuses connection",
			self: pod2,
			peers: map[string]*fakePeer{
				pod0: standalone(pod0),
				pod1: {connectErr: refused},
				pod2: replicaOf(pod0, pod2),
			},
			wantPrimary:  pod0,
			wantReplicas: []string{pod2},
		},
		{
			name: "one peer connects but cannot answer",
			self: pod2,
			peers: map[string]*fakePeer{
				pod0: standalone(pod0),
				pod1: {statusErr: errors.New("performance_schema not ready")},
				pod2: replicaOf(pod0, pod2),
			},
			wantPrimary:  pod0,
			wantReplicas: []string{pod2},
		},
		// The next three are one story: the primary's datacenter goes dark
		// and, while it is dark, a replica elsewhere crashes and restarts.
		// The restarting replica is us. The primary is still in the peer
		// list -- its pod object exists, the headless service still
		// publishes it -- it just does not answer. What the other replica
		// says depends on whether orchestrator has failed over yet.
		{
			// Not yet. Both survivors still name the dark primary as their
			// source (ours came back from persisted replication config).
			// Report that: failover is orchestrator's decision, and
			// replication threads reconnecting to a dead source are harmless
			// under read_only.
			name: "primary dark, no failover yet",
			self: pod2,
			peers: map[string]*fakePeer{
				pod0: {connectErr: refused},
				pod1: replicaOf(pod0, pod1),
				pod2: replicaOf(pod0, pod2),
			},
			wantPrimary:  pod0,
			wantReplicas: []string{pod1, pod2},
		},
		{
			// Orchestrator has already promoted pod-1: it replicates from
			// nobody now. We still carry the stale persisted source, so the
			// derived topology names the dead primary. That is stale but
			// safe: we stay a read-only replica of it, and orchestrator
			// repoints us as it does every replica of a dead master. What
			// must not happen is pod-1 being missed or us being elected.
			name: "primary dark, failover done, our persisted source is stale",
			self: pod2,
			peers: map[string]*fakePeer{
				pod0: {connectErr: refused},
				pod1: standalone(pod1),
				pod2: replicaOf(pod0, pod2),
			},
			wantPrimary:  pod0,
			wantReplicas: []string{pod1, pod2},
		},
		{
			// Same, but the crash took our replication threads with it, so
			// nothing names a source at all. The only peer that answers and
			// is not us is the promoted one; it must come out as primary.
			name: "primary dark, failover done, our replication did not resume",
			self: pod2,
			peers: map[string]*fakePeer{
				pod0: {connectErr: refused},
				pod1: standalone(pod1),
				pod2: stalled(pod2),
			},
			wantPrimary:  pod1,
			wantReplicas: []string{pod2},
		},
		{
			// And with no failover: pod-1 still points at the dark primary,
			// we point nowhere. Its word wins, we get re-attached to the
			// primary it names, and reconnect until that primary is back or
			// orchestrator moves everyone.
			name: "primary dark, no failover, our replication did not resume",
			self: pod2,
			peers: map[string]*fakePeer{
				pod0: {connectErr: refused},
				pod1: replicaOf(pod0, pod1),
				pod2: stalled(pod2),
			},
			wantPrimary:  pod0,
			wantReplicas: []string{pod1, pod2},
		},
		{
			// Nobody replicates: pick the first peer that is not us.
			name: "fresh cluster, no replication anywhere",
			self: pod2,
			peers: map[string]*fakePeer{
				pod0: standalone(pod0),
				pod1: standalone(pod1),
				pod2: standalone(pod2),
			},
			wantPrimary:  pod0,
			wantReplicas: []string{pod1, pod2},
		},
		{
			name:         "single peer is the primary",
			self:         pod0,
			peers:        map[string]*fakePeer{pod0: standalone(pod0)},
			wantPrimary:  pod0,
			wantReplicas: []string{},
		},
		{
			// Every peer down, us included: there is no topology to derive.
			// An empty topology is not a harmless answer: no primary means
			// no donor, and Bootstrap treats "no donor" as "we're on our
			// own" -- replication reset, standalone, writable.
			name: "no peer answers",
			self: pod2,
			peers: map[string]*fakePeer{
				pod0: {connectErr: refused},
				pod1: {connectErr: refused},
				pod2: {connectErr: refused},
			},
			wantErr: "none of the peers",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withPeers(t, tt.peers)
			peerSet := sets.New[string]()
			for host := range tt.peers {
				peerSet.Insert(host)
			}

			primary, replicas, err := getTopology(t.Context(), tt.self, peerSet, "pass", 0)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantPrimary, primary)
			assert.ElementsMatch(t, tt.wantReplicas, replicas)

			for host, p := range tt.peers {
				if p.connectErr == nil {
					assert.True(t, p.closed, "connection to %s not closed", host)
				}
			}
		})
	}
}
