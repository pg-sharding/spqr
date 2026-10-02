package spqrparser

import (
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
func Parse(sql string) ([]Statement, error) {
	tokenizer := NewStringTokenizer(sql)
	if yyParse(tokenizer) != 0 {
		return nil, spqrerror.Newf(spqrerror.PG_SYNTAX_ERROR,
			"failed to parse query \"%s\": %s", sql, tokenizer.LastError).
			Pos(tokenizer.errorPosition())
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
