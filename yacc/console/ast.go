package spqrparser

import (
	"errors"
	"strings"
)

type Show struct {
	Cmd string
}

type ShardingColumn struct {
	ColName string
}

type AddKeyRange struct {
	LowerBound []byte
	UpperBound []byte
	ShardID    string
	KeyRangeID string
}

type SplitKeyRange struct {
	Border         []byte
	KeyRangeFromID string
	KeyRangeID     string
}

type UniteKeyRange struct {
	KeyRangeIDL string
	KeyRangeIDR string
}

type MoveKeyRange struct {
	DestShardID string
	KeyRangeID  string
}

type Add struct {
	KeyRangeID string
}

type Drop struct {
	KeyRangeID string
}

type Lock struct {
	KeyRangeID string
}

type Unlock struct {
	KeyRangeID string
}

type Shard struct {
	Name string
}

type Listen struct {
	addr string
}
type Shutdown struct{}

type Kill struct {
	Cmd string
}

// coordinator

type RegisterRouter struct {
	Addr string
	ID   string
}

type UnregisterRouter struct {
	ID string
}

// The frollowing constants represent SHOW statements.
const (
	ShowDatabasesStr    = "databases"
	ShowRoutersStr      = "routers"
	ShowShardsStr       = "shards"
	ShowShardingColumns = "sharding_columns"
	ShowKeyRangesStr    = "key_ranges"
	KillClientsStr      = "clients"
	ShowPoolsStr        = "pools"
	ShowUnsupportedStr  = "unsupported"
)

// Statement represents a statement.
type Statement interface {
	iStatement()
}

func (*Show) iStatement()           {}
func (*Add) iStatement()            {}
func (*Drop) iStatement()           {}
func (*Lock) iStatement()           {}
func (*Unlock) iStatement()         {}
func (*Shutdown) iStatement()       {}
func (*Listen) iStatement()         {}
func (*MoveKeyRange) iStatement()   {}
func (*SplitKeyRange) iStatement()  {}
func (*UniteKeyRange) iStatement()  {}
func (*ShardingColumn) iStatement() {}
func (*AddKeyRange) iStatement()    {}
func (*Shard) iStatement()          {}
func (*Kill) iStatement()           {}

func (*RegisterRouter) iStatement()   {}
func (*UnregisterRouter) iStatement() {}

var reservedWords = map[string]int{
	"pools":      POOLS,
	"servers":    SERVERS,
	"clients":    CLIENTS,
	"databases":  DATABASES,
	"show":       SHOW,
	"stats":      STATS,
	"kill":       KILL,
	"column":     COLUMN,
	"sharding":   SHARDING,
	"create":     CREATE,
	"add":        ADD,
	"key":        KEY,
	"range":      RANGE,
	"shards":     SHARDS,
	"key_ranges": KEY_RANGES,
	"lock":       LOCK,
	"unlock":     UNLOCK,
	"drop":       DROP,
	"shutdown":   SHUTDOWN,
	"split":      SPLIT,
	"from":       FROM,
	"by":         BY,
	"to":         TO,
	"with":       WITH,
	"unite":      UNITE,
	"listen":     LISTEN,
	"register":   REGISTER,
	"unregister": UNREGISTER,
	"router":     ROUTER,
	"move":       MOVE,
	"routers":    ROUTERS,
}

// Tokenizer is the struct used to generate SQL
// tokens for the parser.
type Tokenizer struct {
	s   string
	pos int

	ParseTree Statement
	LastError string
}

func (t *Tokenizer) Lex(lval *yySymType) int {
	var c rune = ' '

	// skip through all the spaces, both at the ends and in between
	for c == ' ' {
		if t.pos == len(t.s) {
			return 0
		}
		c = rune(t.s[t.pos])
		t.pos += 1
	}

	tok := ""

	// skip through all the spaces, both at the ends and in between
	for c != ' ' {
		if t.pos == len(t.s) {
			break
		}
		tok = tok + string(c)
		c = rune(t.s[t.pos])
		t.pos += 1
	}
	lval.str = tok

	if tp, ok := reservedWords[strings.ToLower(tok)]; ok {
		return tp
	}

	return STRING
}

func (t *Tokenizer) Error(s string) {
	t.LastError = s
}

func NewStringTokenizer(sql string) *Tokenizer {
	return &Tokenizer{s: sql}
}

func setParseTree(yylex interface{}, stmt Statement) {
	yylex.(*Tokenizer).ParseTree = stmt
}

func Parse(sql string) (Statement, error) {

	tokenizer := NewStringTokenizer(sql)
	if yyParse(tokenizer) != 0 {
		return nil, errors.New(tokenizer.LastError)
	}
	ast := tokenizer.ParseTree
	return ast, nil
}
