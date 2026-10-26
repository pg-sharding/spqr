package spqrparser

import (
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

// Tokenizer is the struct used to generate SQL
// tokens for the parser.
type Tokenizer struct {
	s string

	ParseTree []Statement
	LastError string
	l         *Lexer
	lastTok   int
}

func (t *Tokenizer) Error(s string) {
	t.LastError = s
}

func NewStringTokenizer(sql string) *Tokenizer {
	return &Tokenizer{
		s: sql,
		l: NewLexer([]byte(sql)),
	}
}

func (t *Tokenizer) Lex(lval *yySymType) int {
	t.lastTok = t.l.Lex(lval)
	return t.lastTok
}

// errorPosition returns 1-based offset of the token that caused
// the syntax error. On unexpected end of input it points right
// past the last character, like PostgreSQL does.
func (t *Tokenizer) errorPosition() int32 {
	if t.lastTok == 0 {
		return int32(t.l.pe) + 1
	}
	return int32(t.l.ts) + 1
}

func setParseTree(yylex any, stmt []Statement) {
	yylex.(*Tokenizer).ParseTree = stmt
}

// Parse parses console query. On syntax error the returned error is a
// *spqrerror.SpqrError with Position set to the 1-based offset of the
// offending token, so psql can draw the error cursor.
// Valid PostgreSQL queries receive a hint about connecting to the SQL endpoint.
func Parse(sql string) ([]Statement, error) {
	tokenizer := NewStringTokenizer(sql)
	if yyParse(tokenizer) != 0 {
		err := spqrerror.Newf(spqrerror.PG_SYNTAX_ERROR,
			"failed to parse query \"%s\": %s", sql, tokenizer.LastError).
			Pos(tokenizer.errorPosition())
		if _, _, pgErr := lyx.Parse(sql); pgErr == nil {
			err.Hint("To run PostgreSQL queries, connect to your application database on the router's PostgreSQL port instead of the spqr-console database or admin console port.")
		}
		return nil, err
	}
	ast := tokenizer.ParseTree
	return ast, nil
}

func LexString(l *Tokenizer) []int {

	act := make([]int, 0)
	for {
		v := l.Lex(&yySymType{})

		if v == 0 {
			break
		}
		act = append(act, v)
	}

	return act
}
