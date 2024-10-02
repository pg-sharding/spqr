package parser

import "github.com/pg-sharding/lyx/lyx"

type Parser interface {
	Parse(query string) (ParseState, string, error)

	Stmt() lyx.Node
}
