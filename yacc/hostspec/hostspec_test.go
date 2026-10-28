package hostspec

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	cases := []struct {
		name  string
		input string
		exp   HostSpec
	}{
		{
			name:  "host:port",
			input: "localhost:6432",
			exp:   HostSpec{Address: "localhost:6432"},
		},
		{
			name:  "host:port:az",
			input: "localhost:6432:sas",
			exp:   HostSpec{Address: "localhost:6432", AZ: "sas"},
		},
		{
			name:  "host:port ZONE az",
			input: "localhost:6432 ZONE sas",
			exp:   HostSpec{Address: "localhost:6432", AZ: "sas"},
		},
		{
			name:  "bare host",
			input: "localhost",
			exp:   HostSpec{Address: "localhost"},
		},
		{
			name:  "IPv6",
			input: "[::1]:6432",
			exp:   HostSpec{Address: "[::1]:6432"},
		},
		{
			name:  "IPv6 full",
			input: "[2001:db8::1]:6432",
			exp:   HostSpec{Address: "[2001:db8::1]:6432"},
		},
		{
			name:  "IPv6 full ZONE",
			input: "[2001:db8::1]:6432 ZONE vla",
			exp:   HostSpec{Address: "[2001:db8::1]:6432", AZ: "vla"},
		},
		{
			name:  "host:port PRIORITY 5",
			input: "localhost:6432 PRIORITY 5",
			exp:   HostSpec{Address: "localhost:6432", Priority: 5},
		},
		{
			name:  "host:port:az PRIORITY -1",
			input: "localhost:6432:sas PRIORITY -1",
			exp:   HostSpec{Address: "localhost:6432", AZ: "sas", Priority: -1},
		},
		{
			name:  "host:port ZONE az PRIORITY 3",
			input: "localhost:6432 ZONE sas PRIORITY 3",
			exp:   HostSpec{Address: "localhost:6432", AZ: "sas", Priority: 3},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.exp, got)
		})
	}
}
