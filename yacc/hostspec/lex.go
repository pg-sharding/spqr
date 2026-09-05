package hostspec

import (
	"errors"
	"strings"
)

type Lexer struct {
	input      []byte
	pos        int
	inBrackets bool
}

func NewLexer(data []byte) *Lexer {
	return &Lexer{input: data}
}

func isHostnameChar(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-'
}

func isIPv6Char(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') || c == ':'
}

func (lex *Lexer) Lex(lval *yySymType) int {
	for lex.pos < len(lex.input) && (lex.input[lex.pos] == ' ' || lex.input[lex.pos] == '\t') {
		lex.pos++
	}
	if lex.pos >= len(lex.input) {
		return 0
	}

	c := lex.input[lex.pos]
	switch c {
	case '[':
		lex.pos++
		lex.inBrackets = true
		return LBRACKET
	case ']':
		lex.pos++
		lex.inBrackets = false
		return RBRACKET
	}

	if !lex.inBrackets && c == ':' {
		lex.pos++
		return COLON
	}

	if rest := string(lex.input[lex.pos:]); len(rest) >= 4 && strings.EqualFold(rest[:4], "zone") {
		if len(rest) == 4 || rest[4] == ' ' || rest[4] == '\t' {
			lex.pos += 4
			return ZONE
		}
	}

	start := lex.pos
	if lex.inBrackets {
		for lex.pos < len(lex.input) && isIPv6Char(lex.input[lex.pos]) {
			lex.pos++
		}
	} else {
		for lex.pos < len(lex.input) && isHostnameChar(lex.input[lex.pos]) {
			lex.pos++
		}
	}
	if lex.pos > start {
		lval.str = string(lex.input[start:lex.pos])
		return IDENT
	}
	lex.pos++
	return 0
}

func (lex *Lexer) Error(msg string) {}

type Tokenizer struct {
	l      *Lexer
	result HostSpec
	err    string
}

func NewTokenizer(s string) *Tokenizer {
	return &Tokenizer{l: NewLexer([]byte(s))}
}

func (t *Tokenizer) Lex(lval *yySymType) int { return t.l.Lex(lval) }
func (t *Tokenizer) Error(s string)         { t.err = s }

func setResult(yylex any, h HostSpec) { yylex.(*Tokenizer).result = h }

func Parse(s string) (HostSpec, error) {
	t := NewTokenizer(s)
	if rc := yyParse(t); rc != 0 {
		return HostSpec{}, errors.New(t.err)
	}
	return t.result, nil
}
