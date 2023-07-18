package spqrparser

import (
	"errors"
	"fmt"
	"strings"
)

// Tokenizer is the struct used to generate SQL
// tokens for the parser.
type Tokenizer struct {
	s   string
	pos int

	ParseTree Statement
	LastError string
}

func (t *Tokenizer) Lex(lval *yySymType) int {
	if t.pos == len(t.s) {
		return 0
	}

	var c rune

	// fmt.Printf("%s\n", t.s[t.pos:])

	// skip through all the spaces, both at the ends and in between

loop:
	for {
		if t.pos == len(t.s) {
			return 0
		}

		c = rune(t.s[t.pos])

		/* check if token is just some sybm */
		switch c {
		case '(':
			t.pos += 1
			return TOPENBR
		case ')':
			t.pos += 1
			return TCLOSEBR
		// case ':':
		// 	t.pos += 1
		// 	return TCOLON
		// case ',':
		// 	t.pos += 1
		// 	return TCOMMA
		case ';':
			t.pos += 1
			return TSEMICOLON
		// case '.':
		// 	t.pos += 1
		// 	return TDOT
		case ' ':
			fallthrough
		case '\t':
			fallthrough
		case '\n':
			t.pos += 1
		default:
			break loop
		}
	}

	tok := ""

	var balance []rune

	// skip through all the spaces, both at the ends and in between
token_loop:
	for {
		if t.pos == len(t.s) {
			break
		}
		c = rune(t.s[t.pos])
		/* check if token is just some sybm */
		switch c {
		case '(':
			fallthrough
		case ')':
			fallthrough
		// case ':':
		// 	fallthrough
		// case ',':
		// 	fallthrough
		case ';':
			fallthrough
		// case '.':
		// 	fallthrough
		case ' ', '\t', '\n':
			if len(balance) == 0 {
				break token_loop
			} // else append rune to current token
		case '\'', '"':
			if len(balance) > 0 && balance[len(balance)-1] == c {
				balance = balance[:len(balance)-1]
			} else {
				balance = append(balance, c)
			}
		}

		tok = tok + string(c)
		t.pos += 1
	}

	if len(tok) >= 2 && tok[0] == tok[len(tok)-1] && (tok[0] == '\'' || tok[0] == '"') {
		tok = tok[1 : len(tok)-1]
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
		return nil, errors.New(tokenizer.LastError + fmt.Sprintf(" on pos %d", tokenizer.pos))
	}
	ast := tokenizer.ParseTree
	return ast, nil
}
