
%{
package spqrparser

import (
	"crypto/rand"
	"encoding/hex"
)


func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
	  return "", err
	}
	return hex.EncodeToString(bytes), nil
}
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

	kill                   *Kill
	lock                   *Lock
	unlock                 *Unlock

	ds                     *DataspaceDefinition
	kr                     *KeyRangeDefinition
	shard                  *ShardDefinition
	sharding_rule          *ShardingRuleDefinition

	register_router        *RegisterRouter
	unregister_router      *UnregisterRouter
	
	split                  *SplitKeyRange
	move                   *MoveKeyRange
	unite                  *UniteKeyRange

	shutdown               *Shutdown
	listen                 *Listen
	
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

%token <str> POOLS STATS LISTS SERVERS CLIENTS DATABASES BACKEND_CONNECTIONS

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

%type<entrieslist> sharding_rule_argument_list
%type<shruleEntry> sharding_rule_entry

%type<str> sharding_rule_table_clause
%type<str> sharding_rule_column_clause
%type<str> sharding_rule_hash_function_clause

%type <unlock> unlock_stmt
%type <lock> lock_stmt
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
%type<str> ref_name

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
| BACKEND_CONNECTIONS

show_statement_type:
	reserved_keyword
	{
		switch v := string($1); v {
		case DatabasesStr, RoutersStr, PoolsStr, ShardsStr,BackendConnectionsStr, KeyRangesStr, ShardingRules, ClientsStr:
			$$ = v
		default:
			$$ = UnsupportedStr
		}
	}

kill_statement_type:
	reserved_keyword
	{
		switch v := string($1); v {
		case ClientsStr:
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
	|
    DROP KEY RANGE ALL
    {
        $$ = &Drop{Element: &KeyRangeSelector{KeyRangeID: `*`}}
    }
	| DROP sharding_rule_stmt
	{
		$$ = &Drop{Element: $2}
	}
	|
	DROP SHARDING RULE ALL
    {
        $$ = &Drop{Element: &ShardingRuleSelector{ID: `*`}}
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

ref_name:
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
	LOCK key_range_stmt
	{
		$$ = &Lock{KeyRangeID: $2.KeyRangeID}
	}
	// or lock someting else


dataspace_define_stmt:
	DATASPACE internal_id
	{
		$$ = &DataspaceDefinition{ID: $2}
	}

//alter_dataspace_stmt:
//	ALTER DATASPACE dataspace_id ADD SHARDING RULE shrule_id
//	{
//		$$ = &Alter{Element: &AlterDataspace{ID: $3}}
//	}

sharding_rule_define_stmt:
	SHARDING RULE internal_id sharding_rule_table_clause sharding_rule_argument_list
	{
		$$ = &ShardingRuleDefinition{ID: $3, TableName: $4, Entries: $5}
	}
	|
	SHARDING RULE sharding_rule_table_clause sharding_rule_argument_list
	{
		str, err := randomHex(6)
		if err != nil {
			panic(err)
		}
		$$ = &ShardingRuleDefinition{ID:  "shrule"+str, TableName: $3, Entries: $4}
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
	TABLE ref_name
	{
       $$ = $2
    }
	| /*EMPTY*/	{ $$ = ""; }

sharding_rule_column_clause:
	COLUMN ref_name
	{
		$$ = $2
	}
	|
	COLUMNS ref_name
	{
		$$ = $2
	}/* to be backward-compatable*/

sharding_rule_hash_function_clause:
	HASH FUNCTION ref_name
	{
		$$ = $3
	}
	| /*EMPTY*/ { $$ = ""; }


key_range_define_stmt:
	KEY RANGE internal_id FROM key_range_spec_bound TO key_range_spec_bound ROUTE TO internal_id
	{
		$$ = &KeyRangeDefinition{LowerBound: $5, UpperBound: $7, ShardID: $10, KeyRangeID: $3}
	}
	|
	KEY RANGE FROM key_range_spec_bound TO key_range_spec_bound ROUTE TO internal_id
	{
		str, err := randomHex(6)
		if err != nil {
			panic(err)
		}
		$$ = &KeyRangeDefinition{LowerBound: $4, UpperBound: $6, ShardID: $9, KeyRangeID: "kr"+str}
	}


shard_define_stmt:
	SHARD internal_id WITH HOST address
	{
		$$ = &ShardDefinition{Id: $2, Hosts: []string{$5}}
	}
	|
	SHARD WITH HOST address
	{
		str, err := randomHex(6)
		if err != nil {
			panic(err)
		}
		$$ = &ShardDefinition{Id: "shard" + str, Hosts: []string{$4}}
	}


unlock_stmt:
	UNLOCK key_range_stmt
	{
		$$ = &Unlock{KeyRangeID: $2.KeyRangeID}
	}

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

unregister_router_stmt:
	UNREGISTER ROUTER internal_id
	{
		$$ = &UnregisterRouter{ID: $3}
	} 
	|
	UNREGISTER ROUTER ALL
    {
        $$ = &UnregisterRouter{ID: `*`}
    }

%%

