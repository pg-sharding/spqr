
%{
package spqrparser

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/binary"
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
	strlist                []string
	byte                   byte
	bytes                  []byte
	integer                int64
	uinteger               uint
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

	register_router        *RegisterRouter
	unregister_router      *UnregisterRouter
	
	split                  *SplitKeyRange
	move                   *MoveKeyRange
	unite                  *UniteKeyRange
	krbound                *KeyRangeBound

	shutdown               *Shutdown
	listen                 *Listen

	trace                  *TraceStmt
	stoptrace              *StopTraceStmt

	dataspace              *DataspaceDefinition

	attach                 *AttachTable

	sharedRelation         *ShardedRelaion
	sharedRelationList     []*ShardedRelaion

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
// ','
%token<str> TCOMMA

/* any const */
%token<str> SCONST

%token<integer> ICONST

// ';'
%token<str> TSEMICOLON

// '(' & ')'
%token<str> TOPENBR TCLOSEBR

%type<str> operator where_operator

%type<colref> ColRef

%type<str> any_val any_id

%type<integer> any_int

// CMDS
%type <statement> command
%type<bytes> key_range_bound_elem

%type<krbound>  key_range_bound

// routers
%token <str> SHUTDOWN LISTEN REGISTER UNREGISTER ROUTER ROUTE

%token <str> CREATE ADD DROP LOCK UNLOCK SPLIT MOVE COMPOSE SET CASCADE ATTACH
%token <str> SHARDING COLUMN TABLE HASH FUNCTION KEY RANGE DATASPACE
%token <str> SHARDS KEY_RANGES ROUTERS SHARD HOST RULE COLUMNS VERSION
%token <str> BY FROM TO WITH UNITE ALL ADDRESS FOR
%token <str> CLIENT

%token<str> RELATIONS ALTER

%token <str> IDENTITY MURMUR CITY 

%token<str> START STOP TRACE MESSAGES


%token<str> VARCHAR INTEGER INT TYPES

/* any operator */
%token<str> OP

%type<key_range_selector> key_range_stmt
%type<dataspace_selector> dataspace_select_stmt

%type <str> show_statement_type
%type <str> kill_statement_type

%type <show> show_stmt
%type <kill> kill_stmt

%type <drop> drop_stmt
%type <create> create_stmt

%type <trace> trace_stmt
%type <stoptrace> stoptrace_stmt

%type <attach> attach_stmt

%type <ds> dataspace_define_stmt
%type <kr> key_range_define_stmt
%type <shard> shard_define_stmt

%type<str> hash_function_name
%type<str> opt_dataspace

%type<strlist> col_types_list opt_col_types
%type<str> col_types_elem

%type<bool> opt_cascade

%type<strlist> col_list
%type<sharedRelationList> ds_relations_list opt_ds_relations
%type<sharedRelation> ds_relations_elem


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
	create_stmt
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
	| attach_stmt
	{
		setParseTree(yylex, $1)
	}

any_int:
	ICONST {
		$$ = int64($1)
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
		case DatabasesStr, RoutersStr, PoolsStr, ShardsStr,BackendConnectionsStr, KeyRangesStr, ClientsStr, StatusStr, DataspacesStr, VersionStr:
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

drop_stmt:
	DROP key_range_stmt
	{
		$$ = &Drop{Element: $2}
	}
	| DROP KEY RANGE ALL
	{
		$$ = &Drop{Element: &KeyRangeSelector{KeyRangeID: `*`}}
	}
	| DROP DATASPACE ALL opt_cascade
	{
		$$ = &Drop{Element: &DataspaceSelector{ID: `*`}, CascadeDelete: $4}
	}
	| DROP dataspace_select_stmt opt_cascade
	{
		$$ = &Drop{Element: $2, CascadeDelete: $3}
	}

opt_cascade:
	{$$=false} | CASCADE {$$=true}

trace_stmt:
	START TRACE ALL MESSAGES
	{
		$$ = &TraceStmt{All: true}
	} | 
	START TRACE CLIENT any_int {
		$$ = &TraceStmt {
			Client: $4,
		}
	}

stoptrace_stmt:
	STOP TRACE MESSAGES
	{
		$$ = &StopTraceStmt{}
	}


attach_stmt:
	ALTER dataspace_select_stmt ATTACH TABLE ds_relations_elem
	{
		$$ = &AttachTable{
			Relation: $5,
			Dataspace: $2,
		}
	}


create_stmt:
	CREATE dataspace_define_stmt
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
	DATASPACE any_id opt_col_types opt_ds_relations
	{
		$$ = &DataspaceDefinition{
			ID: $2,
			ColTypes: $3,
			Relations: $4,
		}
	}

opt_ds_relations:
	/*nothing*/{
		$$ = nil
	}
	| RELATIONS ds_relations_list {
		$$ = $2
	}

ds_relations_list:
	ds_relations_elem {$$ = []*ShardedRelaion{$1}} | 
	ds_relations_list TCOMMA ds_relations_elem {
		$$ = append($1, $3)
	}

ds_relations_elem:
	// relation name + cols
	any_id TOPENBR col_list TCLOSEBR {
		$$ = &ShardedRelaion{
			Name: $1,
			Columns: $3,
		}
	}


col_list: 
	any_id {
		$$  = []string{$1}
	} | col_list TCOMMA any_id {
		$$  = append($1, $3)
	}

opt_col_types:
	SHARDING COLUMN TYPES col_types_list {
		$$ = $4
	} | { 
		/* empty column types should be prohibited */
		$$ = nil 
	}

col_types_elem:
	VARCHAR {
		$$ = "varchar"
	} | INTEGER {
		$$ = "integer"
	} | INT {
		$$ = "integer"
	}

col_types_list:
	col_types_list TCOMMA col_types_elem {
		$$ = append($1, $3)
	} | col_types_elem {
		$$ = []string {
			$1,
		}
	}

hash_function_name:
	IDENTITY {
		$$ = "identity"
	} | MURMUR HASH {
		$$ = "murmur"
	} | CITY HASH {
		$$ = "city"
	}

opt_dataspace:
    FOR DATASPACE any_id{
        $$ = $3
    }
    | /* EMPTY */ { $$ = "default" }


key_range_bound_elem:
	any_val {
		$$ = []byte($1)
	}
	| any_int {
		buf := make([]byte, 8)
		binary.PutVarint(buf, $1)
		$$ = buf
	}

key_range_bound:
	key_range_bound_elem { 
		$$ = &KeyRangeBound{
			Pivots: [][]byte{
				$1,
			}
		}
	} 
	| key_range_bound TCOMMA key_range_bound_elem {
		$$ = &KeyRangeBound{
			Pivots: append($1.Pivots, $3),
		}
	}

key_range_define_stmt:
	KEY RANGE any_id FROM key_range_bound TO key_range_bound ROUTE TO any_id opt_dataspace
	{
		$$ = &KeyRangeDefinition{LowerBound: $5, UpperBound: $7, ShardID: $10, KeyRangeID: $3, Dataspace: $11}
	}
	| KEY RANGE FROM key_range_bound TO key_range_bound ROUTE TO any_id opt_dataspace
	{
		str, err := randomHex(6)
		if err != nil {
			panic(err)
		}
		$$ = &KeyRangeDefinition{LowerBound: $4, UpperBound: $6, ShardID: $9, KeyRangeID: "kr"+str, Dataspace: $10}
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

key_range_stmt:
	KEY RANGE any_id
	{
		$$ = &KeyRangeSelector{KeyRangeID: $3}
	}

dataspace_select_stmt:
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
	KILL kill_statement_type any_int
	{
		$$ = &Kill{Cmd: $2, Target: $3}
	}
	| KILL CLIENT any_int {
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

