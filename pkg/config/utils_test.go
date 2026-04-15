package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareConfigs(t *testing.T) {
	cases := []struct {
		cfg1          any
		cfg2          any
		hasDiff       []ConfigFieldState
		expectedError string
	}{
		{
			cfg1: &Coordinator{},
			cfg2: &Coordinator{
				LogFileName:      "string",
				PrettyLogging:    true,
				QdbAddrs:         []string{"string"},
				FrontendTLS:      &TLSConfig{SslMode: "string"},
				EtcdMaxSendBytes: 100,
			},
			hasDiff: []ConfigFieldState{
				{
					FieldName:  ".LogFileName",
					FieldValue: "string",
					Applied:    false,
				},
				{
					FieldName:  ".PrettyLogging",
					FieldValue: "true",
					Applied:    false,
				},
				{
					FieldName:  ".QdbAddrs.0",
					FieldValue: "present",
					Applied:    false,
				},
				{
					FieldName:  ".FrontendTLS.SslMode",
					FieldValue: "string",
					Applied:    false,
				},
				{
					FieldName:  ".FrontendTLS.KeyFile",
					FieldValue: "",
					Applied:    true,
				},
				{
					FieldName:  ".FrontendTLS.CertFile",
					FieldValue: "",
					Applied:    true,
				},
				{
					FieldName:  ".FrontendTLS.RootCertFile",
					FieldValue: "",
					Applied:    true,
				},
				{
					FieldName:  ".EtcdMaxSendBytes",
					FieldValue: "100",
					Applied:    false,
				},
			},
		},
		{
			cfg1: &Router{
				FrontendRules: []*FrontendRule{
					{
						DB:  "db1",
						Usr: "test_user",
					},
				},
			},
			cfg2: &Router{
				ShardMapping: map[string]*Shard{
					"sh1": &Shard{
						Type:     "DATA",
						RawHosts: []string{"shard1:6432:a", "shard2:6432:b"},
					},
				},
				FrontendRules: []*FrontendRule{
					{
						DB:  "db1",
						Usr: "usr1",
					},
				},
			},
			hasDiff: []ConfigFieldState{
				{
					FieldName:  ".ShardMapping.sh1",
					FieldValue: "present",
					Applied:    false,
				},
				{
					FieldName:  ".FrontendRules.0.Usr",
					FieldValue: "usr1",
					Applied:    false,
				},
			},
			expectedError: "",
		},
		{
			cfg1:          &Coordinator{},
			cfg2:          &Router{},
			hasDiff:       nil,
			expectedError: "configs have different types",
		},
		{
			cfg1:          &Coordinator{LogFileName: "string"},
			cfg2:          &Coordinator{LogFileName: "string"},
			hasDiff:       nil,
			expectedError: "",
		},
		{
			cfg1:          (*Coordinator)(nil),
			cfg2:          (*Coordinator)(nil),
			expectedError: "",
		},
		{
			cfg1: &Coordinator{LogFileName: "string"},
			cfg2: (*Coordinator)(nil),
			hasDiff: []ConfigFieldState{
				{
					FieldName:  ".LogFileName",
					FieldValue: "",
					Applied:    false,
				},
			},
			expectedError: "",
		},
		{
			cfg1: (*Coordinator)(nil),
			cfg2: &Coordinator{LogFileName: "string"},
			hasDiff: []ConfigFieldState{
				{
					FieldName:  ".LogFileName",
					FieldValue: "string",
					Applied:    false,
				},
			},
			expectedError: "",
		},
	}

	for _, tc := range cases {
		diff, err := CompareConfigs(tc.cfg1, tc.cfg2)
		if tc.expectedError != "" {
			assert.EqualError(t, err, tc.expectedError)
		} else {
			assert.NoError(t, err)
		}
		assert.Subset(t, diff, tc.hasDiff)
	}
}
