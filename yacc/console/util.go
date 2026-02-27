package spqrparser

import (
	"errors"
)

// Tokenizer is the struct used to generate SQL
// tokens for the parser.
type Tokenizer struct {
	s string

	ParseTree Statement
	LastError string
	l         *Lexer
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
	return t.l.Lex(lval)
}

func setParseTree(yylex any, stmt Statement) {
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
