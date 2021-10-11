
%{

package routing

import (
	"strconv"
)

%}

%union {
  empty         struct{}
  statement     Statement
  str           string
  byte          byte
  int           int
  bool          bool
}

%token <str> STRING COMMAND

%token <str> SHOW KILL

%type <statement> command

%token <str> POOLS STATS LISTS SERVERS CLIENTS DATABASES

%token <str> SHUTDOWN LISTEN

%token <str> CREATE ADD DROP LOCK UNLOCK SPLIT MOVE
%token <str>  SHARDING COLUMN KEY RANGE SHARDS KEY_RANGES
%token <str>  BY FROM TO WITH UNITE


%start any_command

%%


any_command:
    command semicolon_opt

semicolon_opt:
/*empty*/ {}
| ';' {}

anything:
/*emptry*/ {} 
| STRING
	{
		$$ = string($1)
	}


command:
    create_sharding_column_stmt
    {
        setParseTree(yylex, $1)
    }
    | add_stmt
    {
        setParseTree(yylex, $1)
    }
    | drop_stmt
    {
        setParseTree(yylex, $1)
    }
    | lock_stmt
    {
        setParseTree(yylex, $1)
    }
    | unlock_stmt
    {
        setParseTree(yylex, $1)
    }
    | show_stmt
    {
        setParseTree(yylex, $1)
    }
    | kill_stmt
    {
        setParseTree(yylex, $1)
    }
    | listen_stmt
     {
         setParseTree(yylex, $1)
     }
    | shutdown_stmt
     {
         setParseTree(yylex, $1)
     }
    | split_key_range_stmt
     {
         setParseTree(yylex, $1)
     }
    | move_key_range_stmt
    {
        setParseTree(yylex, $1)
    }
    | unite_key_range_stmt
    {
       setParseTree(yylex, $1)
    }

reserved_keyword:
INSERT
| SELECT
| DELETE
| UPDATE
| INSERT
| WHERE

a_expr: c_expr { $$ = $1; }
		|

c_expr:  columnref			{ $$ = $1; }
		;

where_clause:
		WHERE a_expr	{ $$ = $2; }
		| /*EMPTY*/		{ $$ = NULL; }
		;


select_using_shkey_stmt:
	SELECT anything FROM table_name where_clause
