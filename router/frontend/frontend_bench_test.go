package frontend

import (
	"testing"

	"github.com/pg-sharding/spqr/router/relay"
)

type teardownBenchExecutor struct {
	relay.QueryStateExecutor
	started bool
}

func (s *teardownBenchExecutor) FinishBlock() { s.started = false }

type teardownBenchRelay struct {
	relay.RelayStateMgr
	executor teardownBenchExecutor
}

func (s *teardownBenchRelay) QueryExecutor() relay.QueryStateExecutor { return &s.executor }
func (s *teardownBenchRelay) CompleteRelay() error                    { return nil }
func (s *teardownBenchRelay) CompleteRelayClient() error              { return nil }

func BenchmarkTeardownFinishBlock(b *testing.B) {
	s := &teardownBenchRelay{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.executor.started = true
		if err := teardownPipeline(s, nil); err != nil {
			b.Fatal(err)
		}
	}
	if s.executor.started {
		b.Fatal("block not finished")
	}
}
