
%{
package spqrparser

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
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

    set                    *Set
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

	trace                  *TraceStmt
	stoptrace              *StopTraceStmt
	
	entrieslist            []ShardingRuleEntry
	shruleEntry            ShardingRuleEntry

	sharding_rule_selector *ShardingRuleSelector
	key_range_selector     *KeyRangeSelector
	dataspace_selector     *DataspaceSelector

    colref                 ColumnRef
    where                  WhereClauseNode
}

// any non-terminal which returns a value needs a type, which is
// really a field name in the above union struct
//%type <val> expr number

// same for terminals
%token <str> IDENT COMMAND

// DDL
%token <str> SHOW KILL

// SQL
%token <str> WHERE OR AND

%type< where> where_clause where_clause_seq

// '='
%token<str> TEQ

/* any const */
%token<str> SCONST

// ';'
%token<str> TSEMICOLON

// '(' & ')'
%token<str> TOPENBR TCLOSEBR

%type<str> operator where_operator

%type<colref> ColRef

%type<str> any_val any_id

// CMDS
%type <statement> command

// routers
%token <str> SHUTDOWN LISTEN REGISTER UNREGISTER ROUTER ROUTE

%token <str> CREATE ADD DROP LOCK UNLOCK SPLIT MOVE COMPOSE SET HARD
%token <str> SHARDING COLUMN TABLE HASH FUNCTION KEY RANGE DATASPACE
%token <str> SHARDS KEY_RANGES ROUTERS SHARD HOST SHARDING_RULES RULE COLUMNS VERSION
%token <str> BY FROM TO WITH UNITE ALL ADDRESS FOR
%token <str> CLIENT

%token<str> START STOP TRACE MESSAGES

/* any operator */
%token<str> OP


%type<sharding_rule_selector> sharding_rule_stmt
%type<key_range_selector> key_range_stmt
%type<dataspace_selector> dataspace_stmt

%type <str> show_statement_type
%type <str> kill_statement_type

%type <set> set_stmt
%type <show> show_stmt
%type <kill> kill_stmt

%type <drop> drop_stmt
%type <create> add_stmt create_stmt

%type <trace> trace_stmt
%type <stoptrace> stoptrace_stmt

%type <ds> dataspace_define_stmt
%type <sharding_rule> sharding_rule_define_stmt
%type <kr> key_range_define_stmt
%type <shard> shard_define_stmt

%type<entrieslist> sharding_rule_argument_list
%type<shruleEntry> sharding_rule_entry

%type<str> sharding_rule_table_clause
%type<str> sharding_rule_column_clause
%type<str> sharding_rule_hash_function_clause
%type<str> opt_dataspace

%type <unlock> unlock_stmt
%type <lock> lock_stmt
%type <shutdown> shutdown_stmt
%type <listen> listen_stmt
%type <split> split_key_range_stmt
%type <move> move_key_range_stmt
%type <unite> unite_key_range_stmt
%type <register_router> register_router_stmt
%type <unregister_router> unregister_router_stmt
%start any_command

%%


any_command:
    command semicolon_opt

semicolon_opt:
/*empty*/ {}
| TSEMICOLON {}


command:
	add_stmt
	{
		setParseTree(yylex, $1)
	}
	| create_stmt
	{
		setParseTree(yylex, $1)
	}
	| trace_stmt
	{
		setParseTree(yylex, $1)
	}
	| stoptrace_stmt
	{
		setParseTree(yylex, $1)
	}
	| set_stmt
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

any_val: SCONST
	{
		$$ = string($1)
	} | 
	IDENT
	{
		$$ = string($1)
	}

any_id: IDENT
	{
		$$ = string($1)
	}


operator:
    IDENT {
        $$ = $1
    } | AND {
        $$ = "AND"
    } | OR {
        $$ = "OR"
    }

where_operator:
    IDENT {
        $$ = $1
    } | TEQ {
        $$ = "="
    }


ColRef:
    any_id {
        $$ = ColumnRef{
            ColName: $1,
        }
    }


where_clause_seq:
    TOPENBR where_clause_seq TCLOSEBR {
        $$ = $2
    } | ColRef where_operator any_val
    {
        $$ = WhereClauseLeaf {
            ColRef:     $1,
			Op:         $2,
            Value:      $3,
        }
    }
    | where_clause_seq operator where_clause_seq
    {
        $$ = WhereClauseOp{
            Op: $2,
            Left: $1,
            Right: $3,
        }
    }

where_clause:
    /* empty */
    {
        $$ = WhereClauseEmpty{}
    }
    | WHERE where_clause_seq
    {
        $$ = $2
    }


show_statement_type:
	IDENT
	{
		switch v := strings.ToLower(string($1)); v {
		case DatabasesStr, RoutersStr, PoolsStr, ShardsStr,BackendConnectionsStr, KeyRangesStr, ShardingRules, ClientsStr, StatusStr, DataspacesStr, VersionStr:
			$$ = v
		default:
			$$ = UnsupportedStr
		}
	}

kill_statement_type:
	IDENT
	{
		switch v := string($1); v {
		case ClientStr:
			$$ = v
		default:
			$$ = "unsupp"
		}
	}

set_stmt:
	SET dataspace_define_stmt
	{
	    $$ = &Set{Element: $2}
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
	|
	DROP dataspace_stmt
	{
		$$ = &Drop{Element: $2, HardDelete = false}
	}
	|
	DROP DATASPACE ALL
	{
		$$ = &Drop{Element: &DataspaceSelector{ID: `*`}, HardDelete = false}
	}
	|
	DROP dataspace_stmt HARD
	{
		$$ = &Drop{Element: $2, HardDelete = true}
	}
	|
	DROP DATASPACE ALL HARD
	{
		$$ = &Drop{Element: &DataspaceSelector{ID: `*`}, HardDelete = true}
	}

add_stmt:
	ADD dataspace_define_stmt
	{
		$$ = &Create{Element: $2}
	}
	|
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


trace_stmt:
	START TRACE ALL MESSAGES
	{
		$$ = &TraceStmt{All: true}
	} | 
	START TRACE CLIENT any_id {
		$$ = &TraceStmt {
			Client: $4,
		}
	}

stoptrace_stmt:
	STOP TRACE MESSAGES
	{
		$$ = &StopTraceStmt{}
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
	SHOW show_statement_type where_clause
	{
		$$ = &Show{Cmd: $2, Where: $3}
	}

lock_stmt:
	LOCK key_range_stmt
	{
		$$ = &Lock{KeyRangeID: $2.KeyRangeID}
	}
	// or lock someting else


dataspace_define_stmt:
	DATASPACE any_id
	{
		$$ = &DataspaceDefinition{ID: $2}
	}

sharding_rule_define_stmt:
	SHARDING RULE any_id sharding_rule_table_clause sharding_rule_argument_list opt_dataspace
	{
		$$ = &ShardingRuleDefinition{ID: $3, TableName: $4, Entries: $5, Dataspace: $6}
	}
	|
	SHARDING RULE sharding_rule_table_clause sharding_rule_argument_list opt_dataspace
	{
		str, err := randomHex(6)
		if err != nil {
			panic(err)
		}
		$$ = &ShardingRuleDefinition{ID:  "shrule"+str, TableName: $3, Entries: $4, Dataspace: $5}
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
	TABLE any_id
	{
       $$ = $2
    }
	| /*EMPTY*/	{ $$ = ""; }

sharding_rule_column_clause:
	COLUMN any_id
	{
		$$ = $2
	}
	|
	COLUMNS any_id
	{
		$$ = $2
	}/* to be backward-compatable*/

sharding_rule_hash_function_clause:
	HASH FUNCTION any_id
	{
		$$ = $3
	}
	| /*EMPTY*/ { $$ = ""; }

opt_dataspace:
    FOR DATASPACE any_id{
        $$ = $3
    }
    | /* EMPTY */ { $$ = "default" }


key_range_define_stmt:
	KEY RANGE any_id FROM any_val TO any_val ROUTE TO any_id opt_dataspace
	{
		$$ = &KeyRangeDefinition{LowerBound: []byte($5), UpperBound: []byte($7), ShardID: $10, KeyRangeID: $3, Dataspace: $11}
	}
	| KEY RANGE FROM any_val TO any_val ROUTE TO any_id opt_dataspace
	{
		str, err := randomHex(6)
		if err != nil {
			panic(err)
		}
		$$ = &KeyRangeDefinition{LowerBound: []byte($4), UpperBound: []byte($6), ShardID: $9, KeyRangeID: "kr"+str, Dataspace: $10}
	}


shard_define_stmt:
	SHARD any_id WITH HOST any_val
	{
		$$ = &ShardDefinition{Id: $2, Hosts: []string{$5}}
	}
	|
	SHARD WITH HOST any_val
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
	SHARDING RULE any_id
	{
		$$ =&ShardingRuleSelector{ID: $3}
	}

key_range_stmt:
	KEY RANGE any_id
	{
		$$ = &KeyRangeSelector{KeyRangeID: $3}
	}

dataspace_stmt:
	DATASPACE any_id
	{
		$$ = &DataspaceSelector{ID: $2}
	}

split_key_range_stmt:
	SPLIT key_range_stmt FROM any_id BY any_val
	{
		$$ = &SplitKeyRange{KeyRangeID: $2.KeyRangeID, KeyRangeFromID: $4, Border: []byte($6)}
	}

kill_stmt:
	KILL kill_statement_type any_val
	{
		$$ = &Kill{Cmd: $2, Target: $3}
	}
	| KILL CLIENT any_val{
		$$ = &Kill{Cmd: "client", Target: $3}
	}

move_key_range_stmt:
	MOVE key_range_stmt TO any_id
	{
		$$ = &MoveKeyRange{KeyRangeID: $2.KeyRangeID, DestShardID: $4}
	}

unite_key_range_stmt:
	UNITE key_range_stmt WITH any_id
	{
		$$ = &UniteKeyRange{KeyRangeIDL: $2.KeyRangeID, KeyRangeIDR: $4}
	}

listen_stmt:
	LISTEN any_val
	{
		$$ = &Listen{addr: $2}
	}

shutdown_stmt:
	SHUTDOWN
	{
		$$ = &Shutdown{}
	}

// coordinator

register_router_stmt:
	REGISTER ROUTER any_id ADDRESS any_val
	{
		$$ = &RegisterRouter{ID: $3, Addr: $5}
	}

unregister_router_stmt:
	UNREGISTER ROUTER any_id
	{
		$$ = &UnregisterRouter{ID: $3}
	} 
	|
	UNREGISTER ROUTER ALL
    {
        $$ = &UnregisterRouter{ID: `*`}
    }
%%

