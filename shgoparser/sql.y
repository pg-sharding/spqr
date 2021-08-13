// Copyright 2011 Bobby Powers. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

// based off of Appendix A from http://dinosaur.compilertools.net/yacc/

%{

package shgoparser

import (
	"bufio"
	"fmt"
	"os"
	"unicode"
)

var regs = make([]int, 26)
var base int

func setParseTree(yylex interface{}, stmt Statement) {
  yylex.(*Tokenizer).ParseTree = stmt
}

%}

// fields inside this union end up as the fields in a structure known
// as ${PREFIX}SymType, of which a reference is passed to the lexer.
%union {
  empty         struct{}
  show          *Show
}

// any non-terminal which returns a value needs a type, which is
// really a field name in the above union struct
//%type <val> expr number

// same for terminals
%token <val> LETTER

// DDL
%token <bytes> SHOW

// CMDS

//%type <statement> command

%token <bytes> POOLS STATS LISTS SERVERS CLIENTS DATABASES
%type <str> show_statement_type
%type <str> show_stmt
%type <bytes> reserved_keyword

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

func Parse(sql string) (Statement, error) {

	tokenizer := NewStringTokenizer(sql)
	if yyParse(tokenizer) != 0 {
		return nil, errors.New(tokenizer.LastError)
	}
	ast := tokenizer.ParseTree
	return ast, nil
}

