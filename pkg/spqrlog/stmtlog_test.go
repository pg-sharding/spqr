package spqrlog

import (
	"testing"
	"time"
)

func TestStmtLoggerShouldLogStatement(t *testing.T) {
	tests := []struct {
		name         string
		minDuration  time.Duration
		stmtDuration time.Duration
		want         bool
	}{
		{
			name:         "logging is disabled",
			minDuration:  -1,
			stmtDuration: time.Hour,
			want:         false,
		},
		{
			name:         "duration equals threshold",
			minDuration:  time.Second,
			stmtDuration: time.Second,
			want:         false,
		},
		{
			name:         "duration exceeds threshold",
			minDuration:  time.Second,
			stmtDuration: time.Second + time.Millisecond,
			want:         true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			logger := NewStmtLogger(tc.minDuration)
			got := logger.shouldLogStatement(tc.stmtDuration)
			if got != tc.want {
				t.Fatalf("shouldLogStatement(%s) = %t, want %t", tc.stmtDuration, got, tc.want)
			}
		})
	}
}
