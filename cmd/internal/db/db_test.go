package db

import (
	"testing"

	apiv1 "github.com/percona/percona-server-mysql-operator/api/v1"
	"github.com/stretchr/testify/assert"
)

func TestDBParams_setDefaults(t *testing.T) {
	tests := []struct {
		name     string
		params   DBParams
		expected DBParams
	}{
		{
			name: "all defaults",
			params: DBParams{
				User: apiv1.UserOperator,
			},
			expected: DBParams{
				User:                apiv1.UserOperator,
				Port:                33062,
				ReadTimeoutSeconds:  3600,
				CloneTimeoutSeconds: 3600,
				SourceRetryCount:    3,
				SourceConnectRetry:  60,
			},
		},
		{
			name: "custom values",
			params: DBParams{
				User:                apiv1.UserOperator,
				Port:                3306,
				ReadTimeoutSeconds:  30,
				CloneTimeoutSeconds: 300,
				SourceConnectRetry:  60,
			},
			expected: DBParams{
				User:                apiv1.UserOperator,
				Port:                3306,
				ReadTimeoutSeconds:  30,
				CloneTimeoutSeconds: 300,
				SourceRetryCount:    3,
				SourceConnectRetry:  60,
			},
		},
		{
			name: "zero port gets default",
			params: DBParams{
				User: apiv1.UserOperator,
				Port: 0,
			},
			expected: DBParams{
				User:                apiv1.UserOperator,
				Port:                33062,
				ReadTimeoutSeconds:  3600,
				CloneTimeoutSeconds: 3600,
				SourceRetryCount:    3,
				SourceConnectRetry:  60,
			},
		},
		{
			name: "zero clone timeout gets default",
			params: DBParams{
				User:                apiv1.UserOperator,
				Port:                3306,
				ReadTimeoutSeconds:  30,
				CloneTimeoutSeconds: 0,
			},
			expected: DBParams{
				User:                apiv1.UserOperator,
				Port:                3306,
				ReadTimeoutSeconds:  30,
				CloneTimeoutSeconds: 3600,
				SourceRetryCount:    3,
				SourceConnectRetry:  60,
			},
		},
		{
			name: "zero source retry count gets default",
			params: DBParams{
				User:             apiv1.UserOperator,
				Port:             3306,
				SourceRetryCount: 0,
			},
			expected: DBParams{
				User:                apiv1.UserOperator,
				Port:                3306,
				ReadTimeoutSeconds:  3600,
				CloneTimeoutSeconds: 3600,
				SourceRetryCount:    3,
				SourceConnectRetry:  60,
			},
		},
		{
			name: "custom source retry count",
			params: DBParams{
				User:             apiv1.UserOperator,
				Port:             3306,
				SourceRetryCount: 7,
			},
			expected: DBParams{
				User:                apiv1.UserOperator,
				Port:                3306,
				ReadTimeoutSeconds:  3600,
				CloneTimeoutSeconds: 3600,
				SourceRetryCount:    7,
				SourceConnectRetry:  60,
			},
		},
		{
			name: "zero source connect retry gets default",
			params: DBParams{
				User:               apiv1.UserOperator,
				Port:               3306,
				SourceConnectRetry: 0,
			},
			expected: DBParams{
				User:                apiv1.UserOperator,
				Port:                3306,
				ReadTimeoutSeconds:  3600,
				CloneTimeoutSeconds: 3600,
				SourceRetryCount:    3,
				SourceConnectRetry:  60,
			},
		},
		{
			name: "custom source connect retry",
			params: DBParams{
				User:               apiv1.UserOperator,
				Port:               3306,
				SourceConnectRetry: 120,
			},
			expected: DBParams{
				User:                apiv1.UserOperator,
				Port:                3306,
				ReadTimeoutSeconds:  3600,
				CloneTimeoutSeconds: 3600,
				SourceRetryCount:    3,
				SourceConnectRetry:  120,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params.setDefaults()
			assert.Equal(t, tt.expected, tt.params)
		})
	}
}

func TestDBParams_DSN(t *testing.T) {
	params := DBParams{
		User:                apiv1.UserOperator,
		Pass:                "testpass",
		Host:                "localhost",
		Port:                3306,
		ReadTimeoutSeconds:  31,
		CloneTimeoutSeconds: 300,
	}

	dsn := params.DSN()

	assert.Contains(t, dsn, "operator")
	assert.Contains(t, dsn, "testpass")
	assert.Contains(t, dsn, "localhost:3306")
	assert.Contains(t, dsn, "performance_schema")
	assert.Contains(t, dsn, "readTimeout=31s")
	assert.Contains(t, dsn, "timeout=10s")
	assert.Contains(t, dsn, "writeTimeout=31s")
}

func TestDBParams_DSN_IPv6Host(t *testing.T) {
	// A bare IPv6 host has to be bracketed in the address or the driver reads
	// its last hextet as the port ("fcff::1:3306" -> host "fcff::1", port "3306"
	// vs host "fcff:", port "1:3306"). Every host a bootstrap sees in an IPv6
	// cluster -- pod IP, peer, donor -- comes in bare.
	tests := []struct {
		name string
		host string
		want string
	}{
		{"ipv6", "fcff:0:675:1::1b70", "tcp([fcff:0:675:1::1b70]:3306)"},
		{"ipv6 loopback", "::1", "tcp([::1]:3306)"},
		{"ipv4", "10.233.77.173", "tcp(10.233.77.173:3306)"},
		{"hostname", "ps-mysql-0.ps-mysql.ns", "tcp(ps-mysql-0.ps-mysql.ns:3306)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := DBParams{User: apiv1.UserOperator, Pass: "x", Host: tt.host, Port: 3306}
			assert.Contains(t, params.DSN(), tt.want)
		})
	}
}
