package parser

import (
	"sync"

	"github.com/pg-sharding/lyx/lyx"
)

type CacheEntry struct {
	ps   ParseState
	comm string
}

// Parser shared accross all relays.
// caches parse result.
type SharedParser struct {
	parseCache sync.Map

	mu         sync.Mutex
	underlying Parser
}

// Stmt implements Parser.
func (shp *SharedParser) Stmt() lyx.Node {
	return shp.underlying.Stmt()
}

var _ Parser = &SharedParser{}

func (shp *SharedParser) Parse(query string) (ParseState, string, error) {
	ce, ok := shp.parseCache.Load(query)
	if ok {
		return ce.(CacheEntry).ps, ce.(CacheEntry).comm, nil
	}

	p := NewQParser()
	state, comm, err := p.Parse(query)

	if err == nil {
		shp.parseCache.Store(query, CacheEntry{
			ps:   state,
			comm: comm,
		})
	}

	return state, comm, err
}

func NewSharedParser() Parser {
	return &SharedParser{
		parseCache: sync.Map{},
		underlying: NewQParser(),
	}
}
