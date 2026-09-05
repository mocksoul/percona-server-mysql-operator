package async

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-server-mysql-operator/cmd/bootstrap/recovery"
	database "github.com/percona/percona-server-mysql-operator/cmd/internal/db"
	"github.com/percona/percona-server-mysql-operator/pkg/innodbcluster"
)

// gtidServer is one side of the GTID comparison. Set arithmetic is done in
// Go over the small sets these tests use ("a:1-5" style, one interval per
// UUID), which is enough to produce every relation CheckReplicaState
// distinguishes without a live server.
type gtidServer struct {
	executed string
	purged   string
}

func (s *gtidServer) GetGTIDExecuted(context.Context) (string, error) { return s.executed, nil }
func (s *gtidServer) GetGTIDPurged(context.Context) (string, error)   { return s.purged, nil }
func (s *gtidServer) GetCloneThreshold(context.Context) (uint64, error) {
	return 0, errors.New("not a group replication member")
}

func (s *gtidServer) GTIDSubtract(_ context.Context, set, subset string) (string, error) {
	return gtidSubtract(set, subset), nil
}

func (s *gtidServer) GTIDSubtractIntersection(_ context.Context, a, b string) (string, error) {
	return gtidSubtract(a, gtidSubtract(a, b)), nil
}

func (s *gtidServer) IsPurgedSubsetOfExecuted(_ context.Context, purged, executed string) (bool, error) {
	return gtidSubtract(purged, executed) == "", nil
}

// gtidSubtract is GTID_SUBTRACT for sets written as "uuid:lo-hi,uuid:lo-hi",
// one interval per UUID. Subtracting a prefix or the whole interval is all
// the scenarios need; anything that would leave a hole is not expressible
// and is treated as "some of it remains", which is enough for the relations
// being tested (empty vs non-empty is what CheckReplicaState looks at).
func gtidSubtract(set, subset string) string {
	sub := parseGTIDs(subset)
	var out []string
	for _, iv := range parseGTIDsOrdered(set) {
		s, ok := sub[iv.uuid]
		switch {
		case !ok:
			out = append(out, iv.String())
		case s.lo <= iv.lo && s.hi >= iv.hi:
			// fully covered
		case s.lo <= iv.lo && s.hi < iv.hi:
			out = append(out, interval{iv.uuid, s.hi + 1, iv.hi}.String())
		case s.lo > iv.lo && s.hi >= iv.hi:
			out = append(out, interval{iv.uuid, iv.lo, s.lo - 1}.String())
		default:
			out = append(out, iv.String())
		}
	}
	return strings.Join(out, ",")
}

type interval struct {
	uuid   string
	lo, hi int
}

func (iv interval) String() string {
	if iv.lo == iv.hi {
		return fmt.Sprintf("%s:%d", iv.uuid, iv.lo)
	}
	return fmt.Sprintf("%s:%d-%d", iv.uuid, iv.lo, iv.hi)
}

func parseGTIDsOrdered(set string) []interval {
	var out []interval
	for part := range strings.SplitSeq(set, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		uuid, rng, _ := strings.Cut(part, ":")
		lo, hi, found := strings.Cut(rng, "-")
		if !found {
			hi = lo
		}
		l, _ := strconv.Atoi(lo)
		h, _ := strconv.Atoi(hi)
		out = append(out, interval{uuid, l, h})
	}
	return out
}

func parseGTIDs(set string) map[string]interval {
	out := map[string]interval{}
	for _, iv := range parseGTIDsOrdered(set) {
		out[iv.uuid] = iv
	}
	return out
}

// withPrimary makes newPrimaryRunner return the given server, or fail with
// connectErr, for the duration of the test.
func withPrimary(t *testing.T, primary *gtidServer, connectErr error) {
	t.Helper()
	orig := newPrimaryRunner
	newPrimaryRunner = func(context.Context, database.DBParams) (recovery.SQLRunner, func(), error) {
		if connectErr != nil {
			return nil, nil, connectErr
		}
		return primary, func() {}, nil
	}
	t.Cleanup(func() { newPrimaryRunner = orig })
}

func TestDecideClone(t *testing.T) {
	dark := errors.New("dial tcp: i/o timeout")

	tests := []struct {
		name       string
		primary    *gtidServer
		connectErr error
		local      *gtidServer
		wantClone  bool
		wantErrant string
	}{
		// The everyday restart: SIGABRT, OOM, node drain. The replica has
		// its data, it is a little behind. Upstream reclones here on every
		// restart after the first; on a 200G dataset that is hours of
		// degraded redundancy for nothing.
		{
			name:      "replica restarts a little behind",
			primary:   &gtidServer{executed: "a:1-1000"},
			local:     &gtidServer{executed: "a:1-990"},
			wantClone: false,
		},
		{
			name:      "replica restarts fully caught up",
			primary:   &gtidServer{executed: "a:1-1000"},
			local:     &gtidServer{executed: "a:1-1000"},
			wantClone: false,
		},
		{
			name:      "fresh volume",
			primary:   &gtidServer{executed: "a:1-1000"},
			local:     &gtidServer{executed: ""},
			wantClone: true,
		},
		{
			// Down long enough that the primary rotated its binlogs away.
			name:      "replica behind past purged binlogs",
			primary:   &gtidServer{executed: "a:1-1000", purged: "a:1-500"},
			local:     &gtidServer{executed: "a:1-400"},
			wantClone: true,
		},
		{
			// Down a while, but the binlogs it needs are still there.
			name:      "replica behind, primary purged only what it already has",
			primary:   &gtidServer{executed: "a:1-1000", purged: "a:1-300"},
			local:     &gtidServer{executed: "a:1-400"},
			wantClone: false,
		},
		{
			// The old primary after a failover, with writes nobody else
			// got. The new primary (b) has moved on.
			name:       "demoted primary with errant transactions",
			primary:    &gtidServer{executed: "a:1-1000,b:1-50"},
			local:      &gtidServer{executed: "a:1-1003"},
			wantClone:  true,
			wantErrant: "a:1001-1003",
		},
		{
			// The old primary after a clean failover: everything it wrote
			// made it out. It is just a replica that is behind now.
			name:      "demoted primary, nothing errant",
			primary:   &gtidServer{executed: "a:1-1000,b:1-50"},
			local:     &gtidServer{executed: "a:1-1000"},
			wantClone: false,
		},

		// The primary's datacenter is dark and we are a replica elsewhere
		// coming back from a crash. There is nothing to compare against.
		// Cloning would not help: the donor would be another replica, as
		// frozen as we are. Keep the data, let replication reconnect when
		// the primary is back or orchestrator has moved it.
		{
			name:       "primary dark, replica has data",
			connectErr: dark,
			local:      &gtidServer{executed: "a:1-990"},
			wantClone:  false,
		},
		{
			// Same, but we have nothing. Then a clone from a frozen replica
			// is still all the data there is, and better than an empty
			// server that will never catch up.
			name:       "primary dark, replica empty",
			connectErr: dark,
			local:      &gtidServer{executed: ""},
			wantClone:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withPrimary(t, tt.primary, tt.connectErr)

			verdict, err := decideClone(t.Context(), "primary", "pass", 0, tt.local)
			require.NoError(t, err)
			assert.Equal(t, tt.wantClone, verdict.clone, verdict.why)
			assert.NotEmpty(t, verdict.why)
			assert.Equal(t, tt.wantErrant, verdict.errant, "errant GTIDs are reported for a diverged replica and nothing else")
		})
	}
}

func TestCloneDecision(t *testing.T) {
	tests := []struct {
		name      string
		state     innodbcluster.ReplicaGtidState
		wantClone bool
	}{
		// The two states where data on disk is good and a clone would
		// throw it away for nothing. On a large dataset that is the
		// difference between a restart taking a minute and taking hours.
		{"identical: keep data", innodbcluster.ReplicaGtidIdentical, false},
		{"recoverable: keep data, replication catches up", innodbcluster.ReplicaGtidRecoverable, false},

		// Empty replica: nothing to keep.
		{"new: clone", innodbcluster.ReplicaGtidNew, true},

		// Behind, and the primary no longer has the binlogs to close the
		// gap. Replication would fail on the first missing GTID.
		{"irrecoverable: clone", innodbcluster.ReplicaGtidIrrecoverable, true},

		// A primary demoted by failover: it has transactions the new
		// primary never saw. Rejoining with them means two histories.
		{"diverged: clone", innodbcluster.ReplicaGtidDiverged, true},

		// A state this code does not know is not a reason to keep data it
		// cannot vouch for.
		{"unknown: clone", innodbcluster.ReplicaGtidState("SOMETHING"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clone, why := cloneDecision(tt.state)
			assert.Equal(t, tt.wantClone, clone)
			assert.NotEmpty(t, why)
		})
	}
}

// The lock is created right before the post-clone restart and must be gone
// after the next pass, whatever that pass decides. A lock that stays keeps
// the heartbeat sidecar waiting forever.
func TestCloneLock(t *testing.T) {
	lock := filepath.Join(t.TempDir(), "clone.lock")

	require.NoError(t, deleteCloneLock(lock), "removing a lock that is not there is not an error")

	require.NoError(t, createCloneLock(lock))
	_, err := os.Stat(lock)
	require.NoError(t, err)

	require.NoError(t, deleteCloneLock(lock))
	_, err = os.Stat(lock)
	assert.True(t, os.IsNotExist(err))
}

// dumpErrant hands mysqlbinlog the GTID filter and every binlog the index
// lists, in order, and keeps whatever it prints. The stand-in echoes its
// arguments so the test can read the exact invocation off the output file.
func TestDumpErrant(t *testing.T) {
	dir := t.TempDir()

	fake := filepath.Join(dir, "mysqlbinlog")
	require.NoError(t, os.WriteFile(fake, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\"\n"), 0o755))
	orig := mysqlbinlogPath
	mysqlbinlogPath = fake
	t.Cleanup(func() { mysqlbinlogPath = orig })

	datadir := filepath.Join(dir, "data")
	require.NoError(t, os.Mkdir(datadir, 0o755))
	index := filepath.Join(datadir, "binlog.index")
	require.NoError(t, os.WriteFile(index, []byte("./binlog.000001\n./binlog.000002\n/abs/binlog.000003\n\n"), 0o644))

	out := filepath.Join(dir, "errant.sql")
	require.NoError(t, dumpErrant(t.Context(), out, "a:1001-1003", index))

	got, err := os.ReadFile(out)
	require.NoError(t, err)
	assert.Equal(t, strings.Join([]string{
		"--include-gtids=a:1001-1003",
		"--verbose",
		filepath.Join(datadir, "binlog.000001"),
		filepath.Join(datadir, "binlog.000002"),
		"/abs/binlog.000003",
	}, "\n")+"\n", string(got))

	info, err := os.Stat(out)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "the dump holds row data; owner-only")

	// Never overwrite an earlier dump: a second diverged restart must not
	// destroy what the first one saved.
	assert.Error(t, dumpErrant(t.Context(), out, "a:1", index))

	// mysqlbinlog failing is reported with its stderr, so the log says why.
	require.NoError(t, os.WriteFile(fake, []byte("#!/bin/sh\necho 'ERROR: bad magic' >&2\nexit 1\n"), 0o755))
	err = dumpErrant(t.Context(), filepath.Join(dir, "second.sql"), "a:1", index)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad magic")

	// An index with nothing in it means there is nothing to dump from, which
	// is an error worth seeing rather than an empty file that looks like
	// "no errant rows".
	empty := filepath.Join(datadir, "empty.index")
	require.NoError(t, os.WriteFile(empty, []byte("\n"), 0o644))
	assert.Error(t, dumpErrant(t.Context(), filepath.Join(dir, "third.sql"), "a:1", empty))
}
