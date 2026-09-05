package async

import (
	"context"
	"log"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/sjmudd/stopwatch"
	"k8s.io/apimachinery/pkg/util/sets"

	apiv1 "github.com/percona/percona-server-mysql-operator/api/v1"
	"github.com/percona/percona-server-mysql-operator/cmd/bootstrap/utils"
	database "github.com/percona/percona-server-mysql-operator/cmd/internal/db"
	mysqldb "github.com/percona/percona-server-mysql-operator/pkg/db"
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

	cloneLock := filepath.Join(mysql.DataMountPath, "clone.lock")
	requireClone, err := isCloneRequired(cloneLock)
	if err != nil {
		return errors.Wrap(err, "check if clone is required")
	}

	log.Printf("Clone required: %t", requireClone)
	if requireClone {
		log.Println("Checking if a clone in progress")
		inProgress, err := db.CloneInProgress(ctx)
		if err != nil {
			return errors.Wrap(err, "check if a clone in progress")
		}

		log.Printf("Clone in progress: %t", inProgress)
		if inProgress {
			return nil
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

	if !requireClone {
		if err := deleteCloneLock(cloneLock); err != nil {
			return errors.Wrap(err, "delete clone lock")
		}
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

func isCloneRequired(file string) (bool, error) {
	_, err := os.Stat(file)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, errors.Wrapf(err, "stat %s", file)
	}

	return false, nil
}

func createCloneLock(file string) error {
	_, err := os.Create(file)
	return errors.Wrapf(err, "create %s", file)
}

func deleteCloneLock(file string) error {
	err := os.Remove(file)
	return errors.Wrapf(err, "remove %s", file)
}
