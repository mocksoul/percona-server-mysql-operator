package haproxy

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bindLine matches one `bind` directive: address, port, and everything after.
// haproxy writes the wildcard IPv6 address as `:::PORT` (`::` plus the port
// separator), so the address group is allowed to be `::` or `*`.
var bindLine = regexp.MustCompile(`^\s*bind\s+(\*|::|[\d.]+|\[[0-9a-fA-F:]+\]):(\d+)(.*)$`)

// readBindDirectives returns every bind directive of build/haproxy-global.cfg
// keyed by port. A port bound twice is a config error worth failing on.
func readBindDirectives(t *testing.T) map[int]struct{ addr, opts string } {
	t.Helper()

	f, err := os.Open(filepath.Join("..", "..", "build", "haproxy-global.cfg"))
	require.NoError(t, err)
	defer f.Close()

	binds := map[int]struct{ addr, opts string }{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		m := bindLine.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		port, err := strconv.Atoi(m[2])
		require.NoError(t, err)
		_, dup := binds[port]
		require.Falsef(t, dup, "port %d is bound twice", port)
		binds[port] = struct{ addr, opts string }{m[1], strings.TrimSpace(m[3])}
	}
	require.NoError(t, sc.Err())
	return binds
}

func TestGlobalConfigBindsDualStack(t *testing.T) {
	// `bind *:PORT` listens on 0.0.0.0 only: in a cluster where pods get IPv6
	// addresses the haproxy pod is unreachable on every port the operator
	// publishes through its Service. `bind :::PORT v4v6` accepts both families
	// on one socket and still works on IPv4-only nodes.
	binds := readBindDirectives(t)

	published := map[string]int{
		"mysql":    PortMySQL,
		"replicas": PortMySQLReplicas,
		"proxy":    PortProxyProtocol,
		"mysqlx":   PortMySQLXProtocol,
		"admin":    PortAdmin,
		"stats":    PortPMMStats,
	}
	for name, port := range published {
		t.Run(name, func(t *testing.T) {
			b, ok := binds[port]
			require.Truef(t, ok, "port %d is published by the operator but not bound in haproxy-global.cfg", port)
			assert.Equalf(t, "::", b.addr, "port %d: bind address must be the IPv6 wildcard", port)
			assert.Containsf(t, strings.Fields(b.opts), "v4v6", "port %d: dual-stack needs the v4v6 option", port)
		})
	}

	// The proxy-protocol frontend is the one haproxy peers use; it keeps its
	// accept-proxy on top of the dual-stack options.
	assert.Contains(t, strings.Fields(binds[PortProxyProtocol].opts), "accept-proxy")
}
