package shgoparser

import (
	"errors"
	"strings"
)

type Show struct {
	Cmd string
}

type Kill struct {
	Cmd string
}

// The frollowing constants represent SHOW statements.
const (
	ShowDatabasesStr   = "databases"
	KillClientsStr     = "clients"
	ShowPoolsStr       = "pools"
	ShowUnsupportedStr = "unsupported"
)

// Statement represents a statement.
type Statement interface {
	iStatement()
}

func (*Show) iStatement() {}

var reserveds map[string]int = map[string]int{
	"pools":     POOLS,
	"servers":   SERVERS,
	"clients":   CLIENTS,
	"databases": DATABASES,
	"show":      SHOW,
	"stats":     STATS,
	"kill":      KILL,
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

	if tp, ok := reserveds[strings.ToLower(tok)]; ok {
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
