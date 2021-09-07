
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
  drop          *Drop
  lock          *Lock
  shutdown      *Shutdown
  unlock        *Unlock
  split         *SplitKeyRange
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

%token <str> CREATE SHARDING COLUMN ADD KEY RANGE SHARDS KEY_RANGES DROP LOCK UNLOCK SHUTDOWN SPLIT BY FROM

%type <str> show_statement_type
%type <str> kill_statement_type

%type <show> show_stmt
%type <kill> kill_stmt

%type <sh_col> create_sharding_column_stmt

%type <kr> add_stmt add_key_range_stmt
%type <drop> drop_stmt drop_key_range_stmt
%type <unlock> unlock_stmt unlock_key_range_stmt
%type <lock> lock_stmt lock_key_range_stmt
%type <shutdown> shutdown_stmt
%type <split> split_key_range_stmt

%type <str> reserved_keyword
%type <str> sharding_column_name

%type<str> key_range_spec_shid
%type<int> key_range_spec_bound
%type<str> key_range_id

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
    | shutdown_stmt
     {
         setParseTree(yylex, $1)
     }
    | split_key_range_stmt
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

key_range_spec_bound:
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

key_range_id:
  STRING
  {
    $$ = string($1)
  }

drop_stmt:
    drop_key_range_stmt

lock_stmt:
    lock_key_range_stmt

add_stmt:
    add_key_range_stmt

unlock_stmt:
    unlock_key_range_stmt

add_key_range_stmt:
    ADD KEY RANGE key_range_spec_bound key_range_spec_bound key_range_spec_shid key_range_id
      {
        $$ = &KeyRange{From: $4, To: $5, ShardID: $6, KeyRangeID: $7}
      }

drop_key_range_stmt:
  DROP KEY RANGE key_range_id
  {
    $$ = &Drop{KeyRangeID: $4}
  }

lock_key_range_stmt:
  LOCK KEY RANGE key_range_id
   {
      $$ = &Lock{KeyRangeID: $4}
   }

unlock_key_range_stmt:
  UNLOCK KEY RANGE key_range_id
   {
      $$ = &Unlock{KeyRangeID: $4}
   }


split_key_range_stmt:
  SPLIT KEY RANGE key_range_id FROM key_range_id BY key_range_spec_bound
   {
      $$ = &SplitKeyRange{KeyRangeID: $4, KeyRangeFromID: $6, Border: $8}
   }

kill_stmt:
KILL kill_statement_type
{
  $$ = &Kill{Cmd: $2}
}

shutdown_stmt:
    SHUTDOWN
    {
        $$ = &Shutdown{}
    }

%%

