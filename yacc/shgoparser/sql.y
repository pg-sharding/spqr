
%{

package shgoparser


%}

// fields inside this union end up as the fields in a structure known
// as ${PREFIX}SymType, of which a reference is passed to the lexer.
%union {
  empty         struct{}
  statement     Statement
  show          *Show
  kill          *Kill
  str           string
  byte          byte
}

// any non-terminal which returns a value needs a type, which is
// really a field name in the above union struct
//%type <val> expr number

// same for terminals
%token <str> STRING COMMAND

// DDL
%token <str> SHOW KILL


// CMDS
//%type <statement> command

%token <str> POOLS STATS LISTS SERVERS CLIENTS DATABASES
%type <str> show_statement_type
%type <str> kill_statement_type

%type <show> show_stmt
%type <kill> kill_stmt
%type <str> reserved_keyword


%left '|'
%left '&'
%left '+'  '-'
%left '*'  '/'  '%'
%left UMINUS      /*  supplies  precedence  for  unary  minus  */

%start any_command

%%

any_command:
  show_stmt semicolon_opt
  {
    setParseTree(yylex, $1)
  }

semicolon_opt:
/*empty*/ {}
| ';' {}


reserved_keyword:
POOLS
| DATABASES
| CLIENTS
| SERVERS
| STATS

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


kill_statement_type:
reserved_keyword
{
  switch v := string($1); v {
  case KillClientsStr:
    $$ = v
  default:
    $$ = "unsupp"
  }
}


show_stmt:
  SHOW show_statement_type
  {
    $$ = &Show{Cmd: $2}
  }

kill_stmt:
KILL kill_statement_type
{
  $$ = &Kill{Cmd: $1}
}

%%

