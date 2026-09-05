package mysql

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const serviceAccountNamespace = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// runCreateDefaultCnf runs create_default_cnf from build/ps-entrypoint.sh with
// a stubbed `hostname` and returns the node.cnf it wrote. Only the function
// is extracted; sourcing the whole script would start the container
// entrypoint. `hostname -f` is what the entrypoint sees before the headless
// service has a record for the pod, or with a hostname that does not resolve.
func runCreateDefaultCnf(t *testing.T, podName, fqdn string) string {
	t.Helper()

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}

	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	require.NoError(t, os.MkdirAll(bin, 0o755))

	hostname := "#!/bin/bash\n" +
		"case \"$1\" in\n" +
		"  -I) echo '10.0.0.5 ' ;;\n" +
		"  -f) echo '" + fqdn + "' ;;\n" +
		"  *) echo '" + podName + "' ;;\n" +
		"esac\n"
	require.NoError(t, os.WriteFile(filepath.Join(bin, "hostname"), []byte(hostname), 0o755))

	entrypoint, err := os.ReadFile(filepath.Join("..", "..", "build", "ps-entrypoint.sh"))
	require.NoError(t, err)
	// Every top-level function definition, so that whatever helpers
	// create_default_cnf calls from the same file are defined too; the
	// top-level statements between them (traps, xtrace, the mysqld exec) stay
	// out.
	var functions strings.Builder
	for _, f := range regexp.MustCompile(`(?ms)^[a-z_]+\(\) \{$.*?^\}$`).FindAll(entrypoint, -1) {
		functions.Write(f)
		functions.WriteByte('\n')
	}
	function := functions.String()
	require.Contains(t, function, "create_default_cnf() {", "create_default_cnf not found in ps-entrypoint.sh")

	// Helpers the function calls live in build/lib/util.sh; releases before
	// that file existed keep everything inside the function itself.
	lib, err := filepath.Abs(filepath.Join("..", "..", "build", "lib", "util.sh"))
	require.NoError(t, err)
	cfg := filepath.Join(dir, "node.cnf")
	namespace := filepath.Join(dir, "namespace")
	require.NoError(t, os.WriteFile(namespace, []byte("test-ns"), 0o644))

	script := strings.Join([]string{
		"set -eo pipefail",
		"if [ -f " + lib + " ]; then . " + lib + "; fi",
		"CFG=" + cfg,
		"TLS_DIR=" + filepath.Join(dir, "no-tls"),
		"CUSTOM_CONFIG_FILES=()",
		"MYSQL_VERSION=8.0",
		"CLUSTER_TYPE=async",
		"CLUSTER_HASH=1234567",
		"SERVICE_NAME=" + podName[:strings.LastIndex(podName, "-")],
		"HOSTNAME=" + podName,
		strings.ReplaceAll(function, serviceAccountNamespace, namespace),
		"create_default_cnf",
	}, "\n")

	cmd := exec.CommandContext(t.Context(), "bash", "-c", script)
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	got, err := os.ReadFile(cfg)
	require.NoError(t, err)

	return string(got)
}

func TestCreateDefaultCnfServerID(t *testing.T) {
	// server_id = CLUSTER_HASH + statefulset ordinal. The ordinal has to come
	// from the pod name alone: `hostname -f` needs the headless service
	// record to exist already, and a startup that races it (or a cluster
	// without working reverse lookup) must not end up with a server_id that
	// is not even numeric.
	tests := []struct {
		name string
		pod  string
		fqdn string
	}{
		{"fqdn resolves", "cluster1-mysql-0", "cluster1-mysql-0.cluster1-mysql.test-ns.svc.cluster.local"},
		{"two-digit ordinal", "cluster1-mysql-12", "cluster1-mysql-12.cluster1-mysql.test-ns.svc.cluster.local"},
		{"dashes in cluster name", "rt-prod-mysql-3", "rt-prod-mysql-3.rt-prod-mysql.test-ns.svc.cluster.local"},
		{"fqdn is the short name", "rt-prod-mysql-3", "rt-prod-mysql-3"},
		{"fqdn lookup fails", "rt-prod-mysql-3", "hostname: Name or service not known"},
		{"fqdn empty", "rt-prod-mysql-3", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cnf := runCreateDefaultCnf(t, tt.pod, tt.fqdn)
			ordinal := tt.pod[strings.LastIndex(tt.pod, "-")+1:]
			assert.Contains(t, cnf, "server_id=1234567"+ordinal+"\n")
			assert.Contains(t, cnf, "report_host="+tt.pod+".")
		})
	}
}
