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

// runPodIP calls pod_ip from build/ps-entrypoint.sh with `hostname -I`
// stubbed to print addrs, and returns what it printed. Only the function is
// extracted; sourcing the whole script would start the container entrypoint.
// env is appended to the process environment, so a POD_IP_*_REGEX given
// there reaches the function.
func runPodIP(t *testing.T, addrs string, env ...string) string {
	t.Helper()

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}

	bin := filepath.Join(t.TempDir(), "bin")
	require.NoError(t, os.MkdirAll(bin, 0o755))
	stub := "#!/bin/bash\necho '" + addrs + "'\n"
	require.NoError(t, os.WriteFile(filepath.Join(bin, "hostname"), []byte(stub), 0o755))

	entrypoint, err := os.ReadFile(filepath.Join("..", "..", "build", "ps-entrypoint.sh"))
	require.NoError(t, err)
	function := regexp.MustCompile(`(?ms)^pod_ip\(\) \{$.*?^\}$`).Find(entrypoint)
	require.NotNil(t, function, "pod_ip not found in ps-entrypoint.sh")

	script := "set -eo pipefail\n" + string(function) + "\npod_ip\n"
	cmd := exec.CommandContext(t.Context(), "bash", "-c", script)
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"))
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out))
}

func TestPodIP(t *testing.T) {
	// A pod on one network: first address, link-local skipped.
	single := "10.0.0.5 "
	linkLocalFirst := "169.254.172.3 10.0.0.5 "
	linkLocalOnly := "169.254.172.3 169.254.172.4 "

	// A pod on several networks, cluster traffic on the last one: the
	// overlay v4 address comes first, the routable v6 one last.
	multi := "10.233.77.173 2a02:6b8:c42:920::1b70 fcff:0:675:1::1b70 "

	tests := []struct {
		name  string
		addrs string
		env   []string
		want  string
	}{
		{"single address", single, nil, "10.0.0.5"},
		{"link-local skipped", linkLocalFirst, nil, "10.0.0.5"},
		{"link-local only falls back to first", linkLocalOnly, nil, "169.254.172.3"},
		{"multi-network without include takes first", multi, nil, "10.233.77.173"},
		{"include picks the matching network", multi, []string{"POD_IP_INCLUDE_REGEX=^fcff:"}, "fcff:0:675:1::1b70"},
		{"include on a prefix that is not ours", multi, []string{"POD_IP_INCLUDE_REGEX=^fcff:0:67:"}, "10.233.77.173"},
		{"include with no match keeps default", multi, []string{"POD_IP_INCLUDE_REGEX=^192\\.168\\."}, "10.233.77.173"},
		{"include never resurrects an excluded address", linkLocalFirst, []string{"POD_IP_INCLUDE_REGEX=^169\\."}, "10.0.0.5"},
		{"custom exclude", multi, []string{"POD_IP_EXCLUDE_REGEX=^10\\."}, "2a02:6b8:c42:920::1b70"},
		{"include is a full regex, several networks match first wins", multi, []string{"POD_IP_INCLUDE_REGEX=^(2a02|fcff):"}, "2a02:6b8:c42:920::1b70"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runPodIP(t, tt.addrs, tt.env...)
			assert.Equal(t, tt.want, got)
			assert.NotContains(t, got, "\n", "pod_ip must print exactly one address")
		})
	}
}
