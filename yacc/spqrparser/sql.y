
%{

package spqrparser

import (

	"strconv"
)

%}

// fields inside this union end up as the fields in a structure known
// as ${PREFIX}SymType, of which a reference is passed to the lexer.
%union {
  empty         struct{}
  statement     Statement
  show          *Show
  kr            *KeyRange
  sh_col        *ShardingColumn
  kill          *Kill
  str           string
  byte          byte
  int           int
  bool          bool
}

// any non-terminal which returns a value needs a type, which is
// really a field name in the above union struct
//%type <val> expr number

// same for terminals
%token <str> STRING COMMAND

// DDL
%token <str> SHOW KILL


// CMDS
%type <statement> command

%token <str> POOLS STATS LISTS SERVERS CLIENTS DATABASES

%token <str> CREATE SHARDING COLUMN ADD KEY RANGE SHARDS KEY_RANGES DROP LOCK UNLOCK

%type <str> show_statement_type
%type <str> kill_statement_type

%type <show> show_stmt
%type <kill> kill_stmt

%type <sh_col> create_sharding_column_stmt

%type <kr> add_key_range_stmt
%type <str> drop_key_range_stmt
%type <str> unlock_key_range_stmt
%type <str> lock_key_range_stmt

%type <str> reserved_keyword
%type <str> sharding_column_name

%type<int> key_range_spec_from
%type<str> key_range_spec_shid
%type<int> key_range_spec_to
%type<string> key_range_id

%start any_command

%%


any_command:
    command semicolon_opt

semicolon_opt:
/*empty*/ {}
| ';' {}


command:
    create_sharding_column_stmt
       {
         setParseTree(yylex, $1)
       }
    | add_key_range_stmt
       {
         setParseTree(yylex, $1)
       }
    | drop_key_range_stmt
       {
         setParseTree(yylex, $1)
       }
    | lock_key_range_stmt
       {
         setParseTree(yylex, $1)
       }
    | unlock_key_range_stmt
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


reserved_keyword:
POOLS
| DATABASES
| CLIENTS
| SERVERS
| SHARDS
| STATS
| KEY_RANGES

show_statement_type:
reserved_keyword
  {
    switch v := string($1); v {
    case ShowDatabasesStr, ShowPoolsStr, ShowShardsStr, ShowKeyRangesStr:
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


sharding_column_name:
  STRING
  {
    $$ = string($1)
  }

key_range_spec_from:
    STRING
    {
      $$, _ = strconv.Atoi(string($1))
    }

key_range_spec_to:
    STRING
    {
      $$, _ = strconv.Atoi(string($1))
    }

key_range_spec_shid:
    STRING
    {
      $$ = string($1)
    }


create_sharding_column_stmt:
    CREATE SHARDING COLUMN sharding_column_name
      {
        $$ = &ShardingColumn{ColName: $4}
      }


add_key_range_stmt:
    ADD KEY RANGE key_range_spec_from key_range_spec_to key_range_spec_shid
      {
        $$ = &KeyRange{From: $4, To: $5, ShardID: $6}
      }


key_range_id:
  STRING
  {
    $$ = string($1)
  }


drop_key_range_stmt:
  DROP KEY RANGE key_range_id
  {
    $$ = string($1)
  }

lock_key_range_stmt:
  LOCK KEY RANGE key_range_id
  {
    $$ = string($1)
  }

unlock_key_range_stmt:
  UNLOCK KEY RANGE key_range_id
  {
    $$ = string($1)
  }

kill_stmt:
KILL kill_statement_type
{
  $$ = &Kill{Cmd: $2}
}

%%

