
%{

package spqrparser


%}

// fields inside this union end up as the fields in a structure known
// as ${PREFIX}SymType, of which a reference is passed to the lexer.
%union {
  empty                  struct{}
  statement              Statement
  show                   *Show
  kr                     *AddKeyRange
  sh_col                 *ShardingColumn
  register_router        *RegisterRouter
  unregister_router      *UnregisterRouter
  kill                   *Kill
  drop                   *Drop
  lock                   *Lock
  shutdown               *Shutdown
  listen                 *Listen
  unlock                 *Unlock
  split                  *SplitKeyRange
  move                   *MoveKeyRange
  unite                  *UniteKeyRange
  str                    string
  byte                   byte
  bytes                []byte
  int                    int
  bool                   bool
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

// routers
%token <str> SHUTDOWN LISTEN REGISTER UNREGISTER ROUTER

%token <str> CREATE ADD DROP LOCK UNLOCK SPLIT MOVE
%token <str>  SHARDING COLUMN KEY RANGE SHARDS KEY_RANGES
%token <str>  BY FROM TO WITH UNITE

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
%type <listen> listen_stmt
%type <split> split_key_range_stmt
%type <move> move_key_range_stmt
%type <unite> unite_key_range_stmt
%type <register_router> register_router_stmt
%type <unregister_router> unregister_router_stmt

%type <str> reserved_keyword
%type <str> sharding_column_name

%type<str> shard_id
%type<str> spqr_addr
%type<bytes> key_range_spec_bound
%type<str> key_range_id
%type<str> router_id
%type<str> router_addr

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
	| register_router_stmt
	{
		setParseTree(yylex, $1)
	}
	| unregister_router_stmt
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
		case ShowDatabasesStr, ShowPoolsStr, ShowShardsStr, ShowKeyRangesStr, ShowShardingColumns:
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
      $$ = []byte($1)
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


shard_id:
	STRING
	{
		$$ = string($1)
	}

spqr_addr:
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
	ADD KEY RANGE key_range_spec_bound key_range_spec_bound shard_id key_range_id
	{
		$$ = &AddKeyRange{LowerBound: $4, UpperBound: $5, ShardID: $6, KeyRangeID: $7}
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

move_key_range_stmt:
	MOVE KEY RANGE key_range_id TO shard_id
	{
		$$ = &MoveKeyRange{KeyRangeID: $4, DestShardID: $5}
	}

unite_key_range_stmt:
	UNITE KEY RANGE key_range_id WITH key_range_id
	{
		$$ = &UniteKeyRange{KeyRangeIDL: $4, KeyRangeIDR: $5}
	}

listen_stmt:
	LISTEN spqr_addr
	{
		$$ = &Listen{addr: $2}
	}

shutdown_stmt:
	SHUTDOWN
	{
		$$ = &Shutdown{}
	}

// coordinator

router_addr:
	STRING
	{
		$$ = string($1)
	}

router_id:
	STRING
	{
		$$ = string($1)
	}

register_router_stmt:
	REGISTER ROUTER router_addr router_id
	{
		$$ = &RegisterRouter{Addr: $3, ID: $4}
	}

unregister_router_stmt:
	UNREGISTER ROUTER router_id
	{
		$$ = &UnregisterRouter{ID: $3}
	}


%%

