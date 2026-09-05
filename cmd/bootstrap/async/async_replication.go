package async

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sjmudd/stopwatch"
	"k8s.io/apimachinery/pkg/util/sets"

	apiv1 "github.com/percona/percona-server-mysql-operator/api/v1"
	"github.com/percona/percona-server-mysql-operator/cmd/bootstrap/recovery"
	"github.com/percona/percona-server-mysql-operator/cmd/bootstrap/utils"
	database "github.com/percona/percona-server-mysql-operator/cmd/internal/db"
	mysqldb "github.com/percona/percona-server-mysql-operator/pkg/db"
	"github.com/percona/percona-server-mysql-operator/pkg/innodbcluster"
	"github.com/percona/percona-server-mysql-operator/pkg/mysql"
)

func Bootstrap(ctx context.Context) error {
	timer := stopwatch.NewNamedStopwatch()
	err := timer.AddMany([]string{"clone", "total"})
	if err != nil {
		return errors.Wrap(err, "add timers")
	}
	timer.Start("total")

	defer func() {
		timer.Stop("total")
		log.Printf("bootstrap finished in %f seconds", timer.ElapsedSeconds("total"))
	}()

	svc := os.Getenv("SERVICE_NAME_UNREADY")
	mysqlSvc := os.Getenv("SERVICE_NAME")
	peers, err := utils.Lookup(svc)
	if err != nil {
		return errors.Wrap(err, "lookup")
	}
	log.Printf("Peers: %v", sets.List(peers))

	fqdn, err := utils.GetFQDN(mysqlSvc)
	if err != nil {
		return errors.Wrap(err, "get FQDN")
	}
	log.Printf("FQDN: %s", fqdn)

	operatorPass, err := utils.GetSecret(apiv1.UserOperator)
	if err != nil {
		return errors.Wrapf(err, "get %s password", apiv1.UserOperator)
	}

	readTimeout, err := utils.GetReadTimeout()
	if err != nil {
		return errors.Wrap(err, "get read timeout")
	}

	primary, replicas, err := getTopology(ctx, fqdn, peers, operatorPass, readTimeout)
	if err != nil {
		return errors.Wrap(err, "get topology")
	}
	log.Printf("Primary: %s Replicas: %v", primary, replicas)

	podHostname, err := os.Hostname()
	if err != nil {
		return errors.Wrap(err, "get hostname")
	}

	podIp, err := utils.GetPodIP(podHostname)
	if err != nil {
		return errors.Wrap(err, "get pod IP")
	}
	log.Printf("PodIP: %s", podIp)

	primaryIp, err := utils.GetPodIP(primary)
	if err != nil {
		return errors.Wrap(err, "get primary IP")
	}
	log.Printf("PrimaryIP: %s", primaryIp)

	donor, err := selectDonor(ctx, fqdn, primary, replicas)
	if err != nil {
		return errors.Wrap(err, "select donor")
	}
	log.Printf("Donor: %s", donor)

	log.Printf("Opening connection to %s", podIp)
	params := database.DBParams{
		User:               apiv1.UserOperator,
		Pass:               operatorPass,
		Host:               podIp,
		ReadTimeoutSeconds: readTimeout,
	}

	cloneTimeout, err := utils.GetCloneTimeout()
	if err != nil {
		return errors.Wrap(err, "get clone timeout")
	}
	params.CloneTimeoutSeconds = cloneTimeout

	sourceRetryCount, err := utils.GetSourceRetryCount()
	if err != nil {
		return errors.Wrap(err, "get source retry count")
	}
	params.SourceRetryCount = sourceRetryCount

	sourceConnectRetry, err := utils.GetSourceConnectRetry()
	if err != nil {
		return errors.Wrap(err, "get source connect retry")
	}
	params.SourceConnectRetry = sourceConnectRetry

	db, err := database.NewDatabase(ctx, params)
	if err != nil {
		return errors.Wrap(err, "connect to database")
	}
	defer db.Close()

	if err := db.StopReplication(ctx); err != nil {
		return err
	}

	switch {
	case donor == "":
		if err := db.ResetReplication(ctx); err != nil {
			return err
		}

		log.Printf("Can't find a donor, we're on our own.")
		return nil
	case donor == fqdn:
		if err := db.ResetReplication(ctx); err != nil {
			return err
		}

		log.Printf("I'm the donor and therefore the primary.")
		return nil
	case primary == fqdn || primaryIp == podIp:
		if err := db.ResetReplication(ctx); err != nil {
			return err
		}

		log.Printf("I'm the primary.")
		return nil
	}

	// clone.lock marks "a clone just finished, mysqld is restarting". The
	// heartbeat sidecar waits for it to go away before it starts. It is made
	// right before the restart and removed on the next pass, before anything
	// else can return early -- a lock left behind keeps heartbeat down, and
	// with it the lag orchestrator reads when it decides whom to promote.
	cloneLock := filepath.Join(mysql.DataMountPath, "clone.lock")
	if err := deleteCloneLock(cloneLock); err != nil {
		return errors.Wrap(err, "delete clone lock")
	}

	log.Println("Checking if a clone in progress")
	inProgress, err := db.CloneInProgress(ctx)
	if err != nil {
		return errors.Wrap(err, "check if a clone in progress")
	}

	log.Printf("Clone in progress: %t", inProgress)
	if inProgress {
		return nil
	}

	verdict, err := decideClone(ctx, primary, operatorPass, readTimeout, &recovery.Runner{DB: db.SQL()})
	if err != nil {
		return errors.Wrap(err, "decide on clone")
	}
	log.Printf("Clone required: %t -- %s", verdict.clone, verdict.why)

	if verdict.clone {
		if verdict.errant != "" {
			// These transactions exist nowhere else. The clone is about to
			// wipe them; keep a copy someone can read later. A failed dump
			// is logged, not fatal: the alternative is a replica that never
			// comes back.
			//
			// The datadir is the right place: CLONE replaces tablespaces,
			// binlogs and the schemas, not stray files (bootstrap.log and
			// auto.cnf survive it), and it is the one volume that is
			// certainly there and certainly big enough.
			out := filepath.Join(mysql.DataMountPath, fmt.Sprintf("errant-%s-%s.sql", fqdn, time.Now().UTC().Format("20060102T150405Z")))
			if err := saveErrant(ctx, db, out, verdict.errant); err != nil {
				log.Printf("WARNING: could not save errant transactions %s: %v", verdict.errant, err)
			} else {
				log.Printf("Saved errant transactions %s to %s", verdict.errant, out)
			}
		}

		if err := db.DisableSuperReadonly(ctx); err != nil {
			return errors.Wrap(err, "disable super read only")
		}

		timer.Start("clone")
		log.Printf("Cloning from %s", donor)
		err = db.Clone(ctx, donor, string(apiv1.UserOperator), operatorPass, mysql.DefaultAdminPort, params.CloneTimeoutSeconds)
		timer.Stop("clone")
		if err != nil && !errors.Is(err, database.ErrRestartAfterClone) {
			return errors.Wrapf(err, "clone from donor %s", donor)
		}

		err = createCloneLock(cloneLock)
		if err != nil {
			return errors.Wrap(err, "create clone lock")
		}

		log.Println("Clone finished. Restarting container...")

		// We return with 1 to restart container
		os.Exit(1)
	}

	rStatus, _, err := db.ReplicationStatus(ctx)
	if err != nil {
		return errors.Wrap(err, "check replication status")
	}

	if rStatus == mysqldb.ReplicationStatusNotInitiated || rStatus == mysqldb.ReplicationStatusStopped {
		log.Println("configuring replication")

		replicaPass, err := utils.GetSecret(apiv1.UserReplication)
		if err != nil {
			return errors.Wrapf(err, "get %s password", apiv1.UserReplication)
		}

		if err := db.StopReplication(ctx); err != nil {
			return errors.Wrap(err, "stop replication")
		}

		if err := db.StartReplication(ctx, primary, replicaPass, mysql.DefaultPort, params.SourceRetryCount, params.SourceConnectRetry); err != nil {
			return errors.Wrap(err, "start replication")
		}
	}

	if err := db.EnableSuperReadonly(ctx); err != nil {
		return errors.Wrap(err, "enable super read only")
	}

	return nil
}

// peerDB is what getTopology needs from a peer. *database.DB satisfies it;
// tests substitute their own through newPeerDB.
type peerDB interface {
	ReplicationStatus(ctx context.Context) (mysqldb.ReplicationStatus, string, error)
	ReportHost(ctx context.Context) (string, error)
	Close() error
}

var newPeerDB = func(ctx context.Context, params database.DBParams) (peerDB, error) {
	return database.NewDatabase(ctx, params)
}

// getTopology asks every peer who it replicates from and returns the primary
// plus the remaining replicas.
//
// A peer that cannot be reached or answered is skipped, not fatal: the pod
// being bootstrapped is itself one of the peers, and so is any other pod
// that is mid-restart. Failing the whole bootstrap on one dead peer means
// two unhealthy pods keep each other down forever -- each one's startup
// probe dies on the other. The topology is derived from whoever answers.
func getTopology(ctx context.Context, fqdn string, peers sets.Set[string], operatorPass string, readTimeout uint32) (string, []string, error) {
	replicas := sets.New[string]()
	primary := ""

	for _, peer := range sets.List(peers) {
		params := database.DBParams{
			User:               apiv1.UserOperator,
			Pass:               operatorPass,
			Host:               peer,
			ReadTimeoutSeconds: readTimeout,
		}

		db, err := newPeerDB(ctx, params)
		if err != nil {
			log.Printf("Skipping peer %s: connect: %v", peer, err)
			continue
		}
		defer db.Close()

		status, source, err := db.ReplicationStatus(ctx)
		if err != nil {
			log.Printf("Skipping peer %s: replication status: %v", peer, err)
			continue
		}

		replicaHost, err := db.ReportHost(ctx)
		if err != nil {
			log.Printf("Skipping peer %s: report_host: %v", peer, err)
			continue
		}
		if replicaHost == "" {
			continue
		}
		replicas.Insert(replicaHost)

		if status == mysqldb.ReplicationStatusActive {
			primary = source
		}
	}

	// The bootstrapped pod is one of the peers and its own mysqld is up by
	// now (the startup probe runs this), so an empty set means not even that
	// worked. It must be an error, not an empty topology: no primary means
	// no donor, and the caller takes "no donor" as "we're on our own" --
	// replication reset, writable, outside the cluster.
	if replicas.Len() == 0 {
		return "", nil, errors.Errorf("none of the peers %v answered", sets.List(peers))
	}

	if primary == "" && peers.Len() == 1 {
		primary = sets.List(peers)[0]
	} else if primary == "" {
		for _, r := range sets.List(replicas) {
			// We should set primary to the first replica, which is not the bootstrapped pod.
			// The bootstrapped pod can't be a primary.
			// Even if it was a primary before, orchestrator will promote another replica "as result of DeadMaster".
			if r != fqdn {
				primary = r
				break
			}
		}
	}

	if replicas.Len() > 0 {
		replicas.Delete(primary)
	}

	return primary, sets.List(replicas), nil
}

func selectDonor(ctx context.Context, fqdn, primary string, replicas []string) (string, error) {
	donor := ""

	operatorPass, err := utils.GetSecret(apiv1.UserOperator)
	if err != nil {
		return "", errors.Wrapf(err, "get %s password", apiv1.UserOperator)
	}

	for _, replica := range replicas {
		params := database.DBParams{
			User: apiv1.UserOperator,
			Pass: operatorPass,
			Host: replica,
		}
		readTimeout, err := utils.GetReadTimeout()
		if err != nil {
			return "", errors.Wrap(err, "get read timeout")
		}
		params.ReadTimeoutSeconds = readTimeout

		db, err := database.NewDatabase(ctx, params)
		if err != nil {
			continue
		}
		db.Close()

		if fqdn != replica {
			donor = replica
			break
		}
	}

	if donor == "" && fqdn != primary {
		donor = primary
	}

	return donor, nil
}

// newPrimaryRunner opens the primary for GTID arithmetic. Tests replace it.
var newPrimaryRunner = func(ctx context.Context, params database.DBParams) (recovery.SQLRunner, func(), error) {
	primaryDB, err := database.NewDatabase(ctx, params)
	if err != nil {
		return nil, nil, err
	}
	return &recovery.Runner{DB: primaryDB.SQL()}, func() { primaryDB.Close() }, nil
}

// decideClone answers "wipe and clone, or keep the data and replicate" for
// this replica, with the reason for the log.
//
// Only the data decides. Whether replication threads are running, whether a
// lock file exists, whether this is the first start -- none of that says
// anything about what is on disk. The data is compared with the primary's by
// GTID set (see recovery.CheckReplicaState).
//
// When the primary cannot be reached there is nothing to compare with, and
// nothing to clone from either: a clone is served by a replica, but a replica
// of a dead primary is frozen at some point in the past, and cloning it
// gains nothing over the data already here. So: keep whatever data there is
// and let replication reconnect; clone only if there is no data at all.
func decideClone(ctx context.Context, primary, operatorPass string, readTimeout uint32, local recovery.SQLRunner) (cloneVerdict, error) {
	primaryRunner, closePrimary, err := newPrimaryRunner(ctx, database.DBParams{
		User:               apiv1.UserOperator,
		Pass:               operatorPass,
		Host:               primary,
		ReadTimeoutSeconds: readTimeout,
	})
	if err != nil {
		executed, gtidErr := local.GetGTIDExecuted(ctx)
		if gtidErr != nil {
			return cloneVerdict{}, errors.Wrap(gtidErr, "get local GTID_EXECUTED")
		}
		if executed == "" {
			return cloneVerdict{clone: true, why: fmt.Sprintf("primary %s unreachable (%v) and no local data", primary, err)}, nil
		}
		return cloneVerdict{why: fmt.Sprintf("primary %s unreachable (%v); keeping local data, replication will reconnect", primary, err)}, nil
	}
	defer closePrimary()

	state, err := recovery.CheckReplicaState(ctx, primaryRunner, local)
	if err != nil {
		return cloneVerdict{}, errors.Wrap(err, "check replica state")
	}

	clone, why := cloneDecision(state)
	verdict := cloneVerdict{clone: clone, why: fmt.Sprintf("%s: %s", state, why)}

	if state == innodbcluster.ReplicaGtidDiverged {
		verdict.errant, err = errantGTIDs(ctx, primaryRunner, local)
		if err != nil {
			return cloneVerdict{}, errors.Wrap(err, "compute errant GTIDs")
		}
	}

	return verdict, nil
}

// cloneVerdict is what decideClone concluded and why. errant is set only for
// a diverged replica: the GTIDs it has and the primary does not.
type cloneVerdict struct {
	clone  bool
	why    string
	errant string
}

// errantGTIDs is GTID_SUBTRACT(local, primary), evaluated on the primary --
// this replica may be read-only and the function is the same everywhere.
func errantGTIDs(ctx context.Context, primary, local recovery.SQLRunner) (string, error) {
	localExecuted, err := local.GetGTIDExecuted(ctx)
	if err != nil {
		return "", errors.Wrap(err, "local GTID_EXECUTED")
	}
	primaryExecuted, err := primary.GetGTIDExecuted(ctx)
	if err != nil {
		return "", errors.Wrap(err, "primary GTID_EXECUTED")
	}
	return primary.GTIDSubtract(ctx, localExecuted, primaryExecuted)
}

// saveErrant asks mysqld where its binlog index is and dumps gtids from it.
func saveErrant(ctx context.Context, db *database.DB, out, gtids string) error {
	var index string
	if err := db.SQL().QueryRowContext(ctx, "SELECT @@log_bin_index").Scan(&index); err != nil {
		return errors.Wrap(err, "get @@log_bin_index")
	}
	return dumpErrant(ctx, out, gtids, index)
}

// mysqlbinlogPath is where the mysql image ships the tool. Tests point it at
// a stand-in.
var mysqlbinlogPath = "mysqlbinlog"

// dumpErrant writes the given GTIDs as SQL to out, from the binlog files
// listed in index (binlog.index in the datadir). Bootstrap runs in the mysqld
// container and sees the files directly; mysqld is up but idle at this
// point, replication is stopped, so the files are complete. The output is
// what a human applies by hand on the new primary after reading it, or
// deletes.
func dumpErrant(ctx context.Context, out, gtids, index string) error {
	binlogs, err := readBinlogIndex(index)
	if err != nil {
		return err
	}
	if len(binlogs) == 0 {
		return errors.Errorf("%s lists no binlogs", index)
	}

	f, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return errors.Wrapf(err, "create %s", out)
	}
	defer f.Close()

	args := append([]string{"--include-gtids=" + gtids, "--verbose"}, binlogs...)
	cmd := exec.CommandContext(ctx, mysqlbinlogPath, args...)
	cmd.Stdout = f
	var stderr strings.Builder
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "mysqlbinlog: %s", strings.TrimSpace(stderr.String()))
	}
	return nil
}

// readBinlogIndex returns the binlog paths from a binlog.index file. Entries
// are relative to the index's directory unless absolute.
func readBinlogIndex(index string) ([]string, error) {
	data, err := os.ReadFile(index)
	if err != nil {
		return nil, errors.Wrapf(err, "read %s", index)
	}

	dir := filepath.Dir(index)
	var out []string
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !filepath.IsAbs(line) {
			line = filepath.Join(dir, line)
		}
		out = append(out, line)
	}
	return out, nil
}

// cloneDecision maps the GTID relation to the verdict. A replica whose SQL
// thread died still has its data and needs replication restarted, not a
// wipe; a replica that was primary until a failover has transactions nobody
// else has and cannot rejoin with them.
func cloneDecision(state innodbcluster.ReplicaGtidState) (bool, string) {
	switch state {
	case innodbcluster.ReplicaGtidIdentical:
		return false, "up to date with the primary"
	case innodbcluster.ReplicaGtidRecoverable:
		return false, "behind the primary, it still has the binlogs we need"
	case innodbcluster.ReplicaGtidNew:
		return true, "no data, the primary has all of it"
	case innodbcluster.ReplicaGtidIrrecoverable:
		return true, "behind the primary and it has purged the binlogs in between"
	case innodbcluster.ReplicaGtidDiverged:
		return true, "we have transactions the primary does not; they cannot be kept"
	default:
		return true, fmt.Sprintf("unknown state %q, cloning to be safe", state)
	}
}

func createCloneLock(file string) error {
	_, err := os.Create(file)
	return errors.Wrapf(err, "create %s", file)
}

// deleteCloneLock removes the lock if present; a missing lock is the usual
// case, not an error.
func deleteCloneLock(file string) error {
	err := os.Remove(file)
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return errors.Wrapf(err, "remove %s", file)
}
