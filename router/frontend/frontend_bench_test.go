package frontend

import (
	"testing"

	"github.com/pg-sharding/spqr/router/relay"
)

type teardownBenchRelay struct {
	relay.RelayStateMgr
	started bool
}

func (s *teardownBenchRelay) FinishBlock()               { s.started = false }
func (s *teardownBenchRelay) CompleteRelay() error       { return nil }
func (s *teardownBenchRelay) CompleteRelayClient() error { return nil }

func BenchmarkTeardownFinishBlock(b *testing.B) {
	s := &teardownBenchRelay{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.started = true
		if err := teardownPipeline(s, nil); err != nil {
			b.Fatal(err)
		}
	}
	if s.started {
		b.Fatal("block not finished")
	}
}
