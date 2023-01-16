
%{

package spqrparser


%}

// fields inside this union end up as the fields in a structure known
// as ${PREFIX}SymType, of which a reference is passed to the lexer.
%union {
	str                    string
	byte                   byte
	bytes                  []byte
	int                    int
	bool                   bool
	empty                  struct{}

	statement              Statement
	show                   *Show

	drop                   *Drop
	create                 *Create

	ds                     *DataspaceDefinition
	kr                     *KeyRangeDefinition
	shard                  *ShardDefinition
	sharding_rule          *ShardingRuleDefinition

	register_router        *RegisterRouter

	unregister_router      *UnregisterRouter
	kill                   *Kill
	dropAll                *DropAll
	lock                   *Lock
	shutdown               *Shutdown
	listen                 *Listen
	unlock                 *Unlock
	split                  *SplitKeyRange
	move                   *MoveKeyRange
	unite                  *UniteKeyRange
	
	entrieslist            []ShardingRuleEntry
	shruleEntry            ShardingRuleEntry

	sharding_rule_selector *ShardingRuleSelector
	key_range_selector     *KeyRangeSelector
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

%token <str> CREATE ADD DROP LOCK UNLOCK SPLIT MOVE COMPOSE
%token <str> SHARDING COLUMN TABLE HASH FUNCTION KEY RANGE DATASPACE
%token <str> SHARDS KEY_RANGES ROUTERS SHARD HOST SHARDING_RULES RULE COLUMNS
%token <str> BY FROM TO WITH UNITE ALL ADDRESS


%type<sharding_rule_selector> sharding_rule_stmt
%type<key_range_selector> key_range_stmt

%type <str> show_statement_type
%type <str> kill_statement_type

%type <show> show_stmt
%type <kill> kill_stmt

%type <drop> drop_stmt
%type <create> add_stmt create_stmt


%type <ds> dataspace_define_stmt
%type <sharding_rule> sharding_rule_define_stmt
%type <kr> key_range_define_stmt
%type <shard> shard_define_stmt


%type <dropAll> drop_key_range_all_stmt drop_sharding_rules_all_stmt unregister_routers_all_stmt

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

%type<str> address
%type<bytes> key_range_spec_bound

%type<str> internal_id

%type<str> router_addr
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
	add_stmt
	{
		setParseTree(yylex, $1)
	}
	| create_stmt
	{
		setParseTree(yylex, $1)
	}
	| drop_stmt
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

drop_stmt:
	DROP key_range_stmt
	{
		$$ = &Drop{Element: $2}
	}
	| DROP sharding_rule_stmt
	{
		$$ = &Drop{Element: $2}
	}

add_stmt:
	ADD sharding_rule_define_stmt
	{
		$$ = &Create{Element: $2}
	}
	|
	ADD key_range_define_stmt
	{
		$$ = &Create{Element: $2}
	}|
	ADD shard_define_stmt
	{
		$$ = &Create{Element: $2}
	}


create_stmt:
	CREATE dataspace_define_stmt
	{
		$$ = &Create{Element: $2}
	}
	|
	CREATE sharding_rule_define_stmt
	{
		$$ = &Create{Element: $2}
	}
	|
	CREATE key_range_define_stmt
	{
		$$ = &Create{Element: $2}
	}|
	CREATE shard_define_stmt
	{
		$$ = &Create{Element: $2}
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

key_range_spec_bound:
    STRING
    {
      $$ = []byte($1)
    }

internal_id:
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


dataspace_define_stmt:
	DATASPACE internal_id
	{
		$$ = &DataspaceDefinition{ID: $2}
	}

//alter_dataspace_stmt:
//	ALTER DATASPACE dataspace_id AD SHARDING RULE shrule_id
//	{
//		$$ = &Alter{Element: &AlterDataspace{ID: $3}}
//	}

sharding_rule_define_stmt:
	SHARDING RULE internal_id sharding_rule_table_clause sharding_rule_argument_list
	{
		$$ = &ShardingRuleDefinition{ID: $3, TableName: $4, Entries: $5}
	}

sharding_rule_argument_list: sharding_rule_entry
    {
      $$ = make([]ShardingRuleEntry, 0)
      $$ = append($$, $1)
    }
    |
    sharding_rule_argument_list sharding_rule_entry
    {
      $$ = append($1, $2)
    }

sharding_rule_entry:
	sharding_rule_column_clause sharding_rule_hash_function_clause
	{
		$$ = ShardingRuleEntry{
			Column: $1,
			HashFunction: $2,
		}
	}

sharding_rule_table_clause:
	TABLE sharding_table_name
	{
       $$ = $2
    }
	| /*EMPTY*/	{ $$ = ""; }

sharding_rule_column_clause:
	COLUMN sharding_column_name
	{
		$$ = $2
	}
	|
	COLUMNS sharding_column_name
	{
		$$ = $2
	}/* to be backward-compatable*/

sharding_rule_hash_function_clause:
	HASH FUNCTION sharding_hash_function_name
	{
		$$ = $3
	}
	| /*EMPTY*/ { $$ = ""; }


key_range_define_stmt:
       KEY RANGE internal_id FROM key_range_spec_bound TO key_range_spec_bound ROUTE TO internal_id
       {
            $$ = &KeyRangeDefinition{LowerBound: $5, UpperBound: $7, ShardID: $10, KeyRangeID: $3}
       }


shard_define_stmt:
	SHARD internal_id WITH HOST address
	{
		$$ = &ShardDefinition{Id: $2, Hosts: []string{$5}}
	}


unlock_stmt:
	unlock_key_range_stmt

sharding_rule_stmt:
	SHARDING RULE internal_id
	{
		$$ =&ShardingRuleSelector{ID: $3}
	}

key_range_stmt:
	KEY RANGE internal_id
	{
		$$ = &KeyRangeSelector{KeyRangeID: $3}
	}

lock_key_range_stmt:
	LOCK key_range_stmt
	{
		$$ = &Lock{KeyRangeID: $2.KeyRangeID}
	}

unlock_key_range_stmt:
	UNLOCK key_range_stmt
	{
		$$ = &Unlock{KeyRangeID: $2.KeyRangeID}
	}


split_key_range_stmt:
	SPLIT key_range_stmt FROM internal_id BY key_range_spec_bound
	{
		$$ = &SplitKeyRange{KeyRangeID: $2.KeyRangeID, KeyRangeFromID: $4, Border: $6}
	}

kill_stmt:
	KILL kill_statement_type
	{
		$$ = &Kill{Cmd: $2}
	}

move_key_range_stmt:
	MOVE key_range_stmt TO internal_id
	{
		$$ = &MoveKeyRange{KeyRangeID: $2.KeyRangeID, DestShardID: $4}
	}

unite_key_range_stmt:
	UNITE key_range_stmt WITH internal_id
	{
		$$ = &UniteKeyRange{KeyRangeIDL: $2.KeyRangeID, KeyRangeIDR: $4}
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


register_router_stmt:
	REGISTER ROUTER internal_id ADDRESS router_addr
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
	UNREGISTER ROUTER internal_id
	{
		$$ = &UnregisterRouter{ID: $3}
	}


%%

