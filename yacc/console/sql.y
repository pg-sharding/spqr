
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
  shard                  *AddShard
  register_router        *RegisterRouter
  unregister_router      *UnregisterRouter
  kill                   *Kill
  drop                   *Drop
  add                    *Add
  dropAll                *DropAll
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
  entrieslist          []ShardingRuleEntry
  shruleEntry            ShardingRuleEntry
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
%token <str> SHUTDOWN LISTEN REGISTER UNREGISTER ROUTER ROUTE

%token <str> CREATE ADD DROP LOCK UNLOCK SPLIT MOVE
%token <str> SHARDING COLUMN TABLE HASH FUNCTION KEY RANGE DATASPACE
%token <str> SHARDS KEY_RANGES ROUTERS SHARD HOST SHARDING_RULES RULE COLUMNS
%token <str> BY FROM TO WITH UNITE ALL ADDRESS

%type <str> show_statement_type
%type <str> kill_statement_type

%type <show> show_stmt
%type <kill> kill_stmt

%type <drop> drop_sharding_colimn_stmt drop_key_range_stmt
%type <dropAll> drop_key_range_all_stmt drop_sharding_rules_all_stmt unregister_routers_all_stmt

%type <add> add_shard_stmt add_key_range_stmt add_sharding_rule_stmt create_dataspace_stmt

%type<entrieslist> sharding_rule_argument_list
%type<shruleEntry> sharding_rule_entry

%type<str> sharding_rule_table_clause
%type<str> sharding_rule_column_clause
%type<str> sharding_rule_hash_function_clause

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

%type<str> shard_id
%type<str> address
%type<bytes> key_range_spec_bound
%type<str> key_range_id
%type<str> router_id
%type<str> router_addr
%type<str> shrule_id
%type<str> dataspace_id
%type<str> sharding_column_name
%type<str> sharding_table_name
%type<str> sharding_hash_function_name

%start any_command

%%


any_command:
    command semicolon_opt

semicolon_opt:
/*empty*/ {}
| ';' {}


command:
	add_shard_stmt
	{
		setParseTree(yylex, $1)
	}
	| add_key_range_stmt
	{
		setParseTree(yylex, $1)
	}
	| add_sharding_rule_stmt
	{
		setParseTree(yylex, $1)
	}
	| drop_key_range_all_stmt
	{
        setParseTree(yylex, $1)
    }
	| drop_sharding_rules_all_stmt
	{
	    setParseTree(yylex, $1)
	}
	| unregister_routers_all_stmt
	{
	    setParseTree(yylex, $1)
	}
	| drop_key_range_stmt
	{
		setParseTree(yylex, $1)
	}
	| drop_sharding_colimn_stmt
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
	| create_dataspace_stmt
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
| ROUTERS
| SHARDING_RULES

show_statement_type:
	reserved_keyword
	{
		switch v := string($1); v {
		case ShowDatabasesStr, ShowRoutersStr, ShowPoolsStr, ShowShardsStr, ShowKeyRangesStr, ShowShardingRules:
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

sharding_table_name:
	STRING
	{
		$$ = string($1)
	}

sharding_hash_function_name:
	STRING
	{
		$$ = string($1)
	}

shrule_id:
	STRING
	{
		$$ = string($1)
	}

dataspace_id:
	STRING
	{
		$$ = string($1)
	}

key_range_spec_bound:
    STRING
    {
      $$ = []byte($1)
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

address:
	STRING
	{
		$$ = string($1)
	}

lock_stmt:
	lock_key_range_stmt

add_key_range_stmt:
	ADD KEY RANGE key_range_id FROM key_range_spec_bound TO key_range_spec_bound ROUTE TO shard_id
	{
		$$ = &Add{Element: &AddKeyRange{LowerBound: $6, UpperBound: $8, ShardID: $11, KeyRangeID: $4}}
	}

create_dataspace_stmt:
	CREATE DATASPACE dataspace_id
	{
		$$ = &Add{Element: &AddDataspace{ID: $3}}
	}

//alter_dataspace_stmt:
//	ALTER DATASPACE dataspace_id AD SHARDING RULE shrule_id
//	{
//		$$ = &Alter{Element: &AlterDataspace{ID: $3}}
//	}

add_sharding_rule_stmt:
	ADD SHARDING RULE shrule_id sharding_rule_argument_list
	{
		$$ = &Add{Element: &AddShardingRule{ID: $4, Entries: $5}}
	}

sharding_rule_argument_list: sharding_rule_entry
    {
      $$ = make([]ShardingRuleEntry, 0)
      $$ = append($$, $1)
    }
    |
    sharding_rule_argument_list ',' sharding_rule_entry
    {
      $$ = append($1, $3)
    };

sharding_rule_entry:
	sharding_rule_table_clause sharding_rule_column_clause sharding_rule_hash_function_clause
	{
		$$ = ShardingRuleEntry{
			Table: $1,
			Column: $2,
			HashFunction: $3,
		}
	}

sharding_rule_table_clause:
	TABLE sharding_table_name
	| /*EMPTY*/	{ $$ = ""; }

sharding_rule_column_clause:
	COLUMN sharding_column_name

sharding_rule_hash_function_clause:
	HASH FUNCTION sharding_hash_function_name
	| /*EMPTY*/ { $$ = ""; }

add_shard_stmt:
	ADD SHARD shard_id WITH HOST address
	{
		$$ = &Add{Element: &AddShard{Id: $3, Hosts: []string{$6}}}
	}

unlock_stmt:
	unlock_key_range_stmt

drop_sharding_colimn_stmt:
	DROP SHARDING RULE shrule_id
	{
		$$ = &Drop{Element: &DropShardingRule{ID: $4}}
	}

drop_key_range_stmt:
	DROP KEY RANGE key_range_id
	{
		$$ = &Drop{Element: &DropKeyRange{KeyRangeID: $4}}
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
		$$ = &MoveKeyRange{KeyRangeID: $4, DestShardID: $6}
	}

unite_key_range_stmt:
	UNITE KEY RANGE key_range_id WITH key_range_id
	{
		$$ = &UniteKeyRange{KeyRangeIDL: $4, KeyRangeIDR: $5}
	}

listen_stmt:
	LISTEN address
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
	REGISTER ROUTER router_id ADDRESS router_addr
	{
		$$ = &RegisterRouter{ID: $3, Addr: $5}
	}

drop_key_range_all_stmt:
    DROP KEY RANGE ALL
    {
        $$ = &DropAll{Entity: EntityKeyRanges}
    }


drop_sharding_rules_all_stmt:
    DROP SHARDING RULE ALL
    {
        $$ = &DropAll{Entity: EntityShardingRule}
    }


unregister_routers_all_stmt:
    UNREGISTER ROUTER ALL
    {
        $$ = &DropAll{Entity: EntityRouters}
    }

unregister_router_stmt:
	UNREGISTER ROUTER router_id
	{
		$$ = &UnregisterRouter{ID: $3}
	}


%%

