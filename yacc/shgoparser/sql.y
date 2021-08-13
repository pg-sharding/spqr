
%{

package shgoparser

import (
	"errors"
	"unicode"
)

type Show struct {
	Cmd string
}

// The frollowing constants represent SHOW statements.
const (
	ShowDatabasesStr     = "databases"
	ShowPoolsStr         = "pools"
	ShowUnsupportedStr   = "unsupported"
)


// Statement represents a statement.
type Statement interface {
	iStatement()
}


func (*Show) iStatement()          {}

// Tokenizer is the struct used to generate SQL
// tokens for the parser.
type Tokenizer struct {
	s      string
	pos    int

	ParseTree     Statement
	LastError     string
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

	// only look for input characters that are either digits or lower case
	// to do more specific parsing, you'll define more tokens and have a
	// more complex parsing logic here, choosing which token to return
	// based on parsed input
	if unicode.IsDigit(c) || unicode.IsLower(c) {
		lval.str = string(c)
		return LETTER
	}

	// do not return any token in case of unrecognized grammer
	// this results in syntax error
	return 0
}

func (t *Tokenizer) Error(s string) {
    return
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



%}

// fields inside this union end up as the fields in a structure known
// as ${PREFIX}SymType, of which a reference is passed to the lexer.
%union {
  empty         struct{}
  show          *Show
  str           string
  byte          byte
}

// any non-terminal which returns a value needs a type, which is
// really a field name in the above union struct
//%type <val> expr number

// same for terminals
%token <val> LETTER

// DDL
%token <str> SHOW

// CMDS

//%type <statement> command

%token <str> POOLS STATS LISTS SERVERS CLIENTS DATABASES
%type <str> show_statement_type
%type <show> show_stmt
%type <str> reserved_keyword

%left '|'
%left '&'
%left '+'  '-'
%left '*'  '/'  '%'
%left UMINUS      /*  supplies  precedence  for  unary  minus  */
%start show_stmt

%%

string: 
	  | LETTER string
;

reserved_keyword:
POOLS
| DATABASES
| CLIENTS
| SERVERS
| STATS
;

show_statement_type:
reserved_keyword
  {
    switch v := string($1); v {
    case ShowDatabasesStr, ShowPoolsStr:
      $$ = v
    default:
      $$ = ShowUnsupportedStr
    }
  }
;

show_stmt:
  SHOW show_statement_type
  {
    $$ = &Show{Cmd: $1}
  }
;


%%

