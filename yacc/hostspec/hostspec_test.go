package hostspec

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		exp    HostSpec
		errMsg string
	}{
		// Basic formats
		{
			name:  "host:port",
			input: "localhost:6432",
			exp:   HostSpec{Address: "localhost:6432"},
		},
		{
			name:  "host:port:az (legacy)",
			input: "localhost:6432:sas",
			exp:   HostSpec{Address: "localhost:6432", AZ: "sas"},
		},
		{
			name:  "host:port ZONE az",
			input: "localhost:6432 ZONE sas",
			exp:   HostSpec{Address: "localhost:6432", AZ: "sas"},
		},
		{
			name:  "bare host without port",
			input: "localhost",
			exp:   HostSpec{Address: "localhost"},
		},

		// IPv4
		{
			name:  "IPv4 host:port",
			input: "127.0.0.1:6432",
			exp:   HostSpec{Address: "127.0.0.1:6432"},
		},
		{
			name:  "IPv4 host:port:az",
			input: "127.0.0.1:6432:klg",
			exp:   HostSpec{Address: "127.0.0.1:6432", AZ: "klg"},
		},
		{
			name:  "IPv4 host:port ZONE az",
			input: "127.0.0.1:6432 ZONE klg",
			exp:   HostSpec{Address: "127.0.0.1:6432", AZ: "klg"},
		},

		// IPv6 bracketed
		{
			name:  "IPv6 bracketed host:port",
			input: "[::1]:6432",
			exp:   HostSpec{Address: "[::1]:6432"},
		},
		{
			name:  "IPv6 bracketed host:port:az",
			input: "[::1]:6432:sas",
			exp:   HostSpec{Address: "[::1]:6432", AZ: "sas"},
		},
		{
			name:  "IPv6 bracketed host:port ZONE az",
			input: "[::1]:6432 ZONE sas",
			exp:   HostSpec{Address: "[::1]:6432", AZ: "sas"},
		},
		{
			name:  "IPv6 full address host:port",
			input: "[2001:db8::1]:6432",
			exp:   HostSpec{Address: "[2001:db8::1]:6432"},
		},
		{
			name:  "IPv6 full address host:port:az",
			input: "[2001:db8::1]:6432:vla",
			exp:   HostSpec{Address: "[2001:db8::1]:6432", AZ: "vla"},
		},
		{
			name:  "IPv6 full address host:port ZONE az",
			input: "[2001:db8::1]:6432 ZONE vla",
			exp:   HostSpec{Address: "[2001:db8::1]:6432", AZ: "vla"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input)
			if tt.errMsg != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.exp, got)
		})
	}
}
