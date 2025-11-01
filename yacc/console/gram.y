
%{
package spqrparser

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/binary"
	"strings"
	"math"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rfqn"
)

const SIGNED_INT_RANGE_ERROR string = "the Signed Value should be at the range of [-9223372036854775808, 9223372036854775807]."

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
	integer                int
	uinteger               uint
	uintegerlist           []uint
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


	krbound                *KeyRangeBound

	qname		   		   *rfqn.RelationFQN
	ds                     *DistributionDefinition
	kr                     *KeyRangeDefinition
	shard                  *ShardDefinition
	sharding_rule          *ShardingRuleDefinition

	register_router        *RegisterRouter
	unregister_router      *UnregisterRouter
	
	split                  *SplitKeyRange
	move                   *MoveKeyRange
	unite                  *UniteKeyRange
	
	redistribute           *RedistributeKeyRange

	invalidate             *Invalidate
	sync_reference_tables  *SyncReferenceTables

	shutdown               *Shutdown
	listen                 *Listen

	trace                  *TraceStmt
	stoptrace              *StopTraceStmt

	distribution           *DistributionDefinition

	alter                  *Alter
	alter_distribution     *AlterDistribution
	distributed_relation   *DistributedRelation
	alter_default_shard    *AlterDefaultShard
	
	relations              []*DistributedRelation
	relation               *DistributedRelation
	entrieslist            []ShardingRuleEntry
	dEntrieslist 	       []DistributionKeyEntry

	shruleEntry            ShardingRuleEntry

	distrKeyEntry          DistributionKeyEntry
	aiEntry                *AutoIncrementEntry

	sharding_rule_selector *ShardingRuleSelector
	key_range_selector     *KeyRangeSelector
	distribution_selector  *DistributionSelector
	aiEntrieslist          []*AutoIncrementEntry

    colref                 ColumnRef
	colreflist			   []ColumnRef
    where                  WhereClauseNode

	order_clause 		   OrderClause
	opt_asc_desc		   OptAscDesc

	group_clause		   GroupByClause

	retryMoveTaskGroup     *RetryMoveTaskGroup
	stopMoveTaskGroup      *StopMoveTaskGroup

	typedColRef         	TypedColRef
	routingExpr				[]TypedColRef

	alter_relation          *AlterRelationV2
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
// '.'
%token<str> TDOT

/* any const */
%token<str> SCONST

%token<uinteger> ICONST
%token<uinteger> INVALID_ICONST


%type<bytes> key_range_bound_elem

%type<krbound>  key_range_bound

// ';'
%token<str> TSEMICOLON
// '-'
%token<str> TMINUS
// '+'
%token<str> TPLUS

// '(' & ')', '[' & ']'
%token<str> TOPENBR TCLOSEBR TOPENSQBR  TCLOSESQBR

%type<str> operator where_operator

%type<colref> ColRef
%type<colreflist> ColRef_list

%type<str> any_val any_id shard_id

%type<uinteger> any_uint

// CMDS
%type <statement> command

// routers
%token <str> SHUTDOWN LISTEN REGISTER UNREGISTER ROUTER ROUTE

%token <str> CREATE ADD DROP LOCK UNLOCK SPLIT MOVE COMPOSE SET CASCADE ATTACH ALTER DETACH REDISTRIBUTE REFERENCE CHECK APPLY
%token <str> SHARDING COLUMN TABLE TABLES RELATIONS BACKENDS HASH FUNCTION KEY RANGE DISTRIBUTION RELATION REPLICATED AUTO INCREMENT SEQUENCE SCHEMA
%token <str> SHARDS KEY_RANGES ROUTERS SHARD HOST SHARDING_RULES RULE COLUMNS VERSION HOSTS SEQUENCES IS_READ_ONLY MOVE_STATS
%token <str> BY FROM TO WITH UNITE ALL ADDRESS FOR
%token <str> CLIENT
%token <str> BATCH SIZE
%token <str> INVALIDATE CACHE
%token <str> SYNC
%token <str> RETRY
%token <str> DISTRIBUTED IN ON
%token <str> DEFAULT

%token <str> IDENTITY MURMUR CITY 

%token<str> START STOP TRACE MESSAGES

%token<str> TASK GROUP

%token<str> VARCHAR INTEGER INT TYPES UUID

/* any operator */
%token<str> OP


%type<sharding_rule_selector> sharding_rule_stmt
%type<key_range_selector> key_range_stmt
%type<distribution_selector> distribution_select_stmt

%type <str> show_statement_type
%type <str> kill_statement_type

%type <show> show_stmt
%type <kill> kill_stmt

%type <drop> drop_stmt
%type <create> add_stmt create_stmt

%type <trace> trace_stmt
%type <stoptrace> stoptrace_stmt

%type<qname> qualified_name
%type <ds> distribution_define_stmt
%type <sharding_rule> sharding_rule_define_stmt
%type <kr> key_range_define_stmt
%type <shard> shard_define_stmt

%type<entrieslist> sharding_rule_argument_list
%type<dEntrieslist> distribution_key_argument_list
%type<aiEntrieslist> opt_auto_increment
%type<aiEntrieslist> auto_inc_argument_list
%type<uinteger> opt_auto_increment_start_clause
%type<shruleEntry> sharding_rule_entry
%type<str> opt_schema_name
%type<distribution_selector> opt_distribution_selector

%type<distrKeyEntry> distribution_key_entry routing_expr
%type<aiEntry> auto_increment_entry

%type<str> sharding_rule_table_clause
%type<str> sharding_rule_column_clause
%type<str> opt_hash_function_clause hash_function_clause
%type<str> hash_function_name

%type<alter> alter_stmt create_distributed_relation_stmt
%type<alter_distribution> distribution_alter_stmt

%type<invalidate> invalidate_stmt
%type<sync_reference_tables> sync_reference_tables_stmt

%type<relations> relation_attach_stmt
%type<relations> distributed_relation_list_def

%type<distributed_relation> distributed_relation_def

%type<alter_relation> relation_alter_stmt_v2

%type<strlist> col_types_list opt_col_types any_id_list opt_on_shards
%type<str> col_types_elem
%type<bool> opt_cascade
%type<str> opt_default_shard


%token <str> ASC DESC ORDER
%type <order_clause> order_clause
%type <opt_asc_desc> opt_asc_desc
%type <group_clause> group_clause
%type <unlock> unlock_stmt
%type <lock> lock_stmt
%type <shutdown> shutdown_stmt
%type <listen> listen_stmt
%type <split> split_key_range_stmt
%type <move> move_key_range_stmt
%type <redistribute> redistribute_key_range_stmt
%type <unite> unite_key_range_stmt
%type <register_router> register_router_stmt
%type <unregister_router> unregister_router_stmt
%type <integer> 	 opt_batch_size

%type <typedColRef>  typed_col_ref
%type <routingExpr> routing_expr_column_list

%type <retryMoveTaskGroup> retry_move_task_group
%type <stopMoveTaskGroup> stop_move_task_group

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
	| redistribute_key_range_stmt
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
	| alter_stmt
	{
		setParseTree(yylex, $1)
	}
	| invalidate_stmt
	{
		setParseTree(yylex, $1)
	} 
	| retry_move_task_group
	{
		setParseTree(yylex, $1)
	}
	| stop_move_task_group
	{
		setParseTree(yylex, $1)
	}
	| sync_reference_tables_stmt
	{
		setParseTree(yylex, $1)
	}
	| create_distributed_relation_stmt
	{
		setParseTree(yylex, $1)
	}

any_uint:
	ICONST {
		$$ = uint($1)
	}

any_val: SCONST
	{
		$$ = string($1)
	} | 
	IDENT
	{
		$$ = string($1)
	} | ICONST {
		if $1 > uint(math.MaxInt64) {
			yylex.Error(SIGNED_INT_RANGE_ERROR)
			return 1
		} else {
			buf := make([]byte, binary.MaxVarintLen64)
			binary.PutVarint(buf, int64($1))
			$$ = string(buf)
		}
	} | TMINUS ICONST {
		if $2 > uint(-math.MinInt64) {
			yylex.Error(SIGNED_INT_RANGE_ERROR)
			return 1
		} else {
			buf := make([]byte, binary.MaxVarintLen64)
			binary.PutVarint(buf, int64(-$2))
			$$ = string(buf)
		}
	}

any_id: IDENT
	{
		$$ = string($1)
	} | SCONST
	{
		$$ = string($1)
	}


shard_id: IDENT
	{
		$$ = string($1)
	} | SCONST
	{
		$$ = string($1)
	}

qualified_name:
	IDENT
	{
		$$ = &rfqn.RelationFQN{RelationName: $1}
	} |
	SCONST
	{
		$$ = &rfqn.RelationFQN{RelationName: $1}
	} |
	IDENT TDOT IDENT
	{
		$$ = &rfqn.RelationFQN{RelationName: $3, SchemaName: $1}
	}


operator:
    OP {
        $$ = $1
    } | AND {
        $$ = "AND"
    } | OR {
        $$ = "OR"
    }

where_operator:
    OP {
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

ColRef_list: 
    ColRef_list TCOMMA ColRef
    {
      $$ = append($1, $3)
    } | ColRef {
      $$ = []ColumnRef {
		  $1,
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
		case DatabasesStr, RoutersStr, PoolsStr, InstanceStr, ShardsStr, BackendConnectionsStr, KeyRangesStr, ShardingRules, ClientsStr, StatusStr, DistributionsStr, CoordinatorAddrStr, VersionStr, ReferenceRelationsStr, TaskGroupStr, PreparedStatementsStr, QuantilesStr, SequencesStr, IsReadOnlyStr, MoveStatsStr, TsaCacheStr, Users, MoveTaskStr:
			$$ = v
		default:
			$$ = UnsupportedStr
		}
	} | RELATIONS {
		$$ = $1
	} | HOSTS {
		$$ = $1
	} | SHARDS {
		$$ = $1
	}

kill_statement_type:
	CLIENT 
	{
		$$ = string($1)
	}
	| IDENT
	{
		switch v := string($1); v {
		case BackendStr:
			$$ = v
		default:
			$$ = UnsupportedStr
		}
	}

opt_cascade:
	CASCADE { $$ = true } | {$$ = false}

drop_stmt:
	DROP key_range_stmt
	{
		$$ = &Drop{Element: $2}
	}
	| DROP KEY RANGE ALL
	{
		$$ = &Drop{Element: &KeyRangeSelector{KeyRangeID: `*`}}
	}
	| DROP sharding_rule_stmt
	{
		$$ = &Drop{Element: $2}
	}
	| DROP SHARDING RULE ALL
	{
		$$ = &Drop{Element: &ShardingRuleSelector{ID: `*`}}
	}
	| DROP distribution_select_stmt opt_cascade
	{
		$$ = &Drop{Element: $2, CascadeDelete: $3}
	}
	| DROP DISTRIBUTION ALL opt_cascade
	{
		$$ = &Drop{Element: &DistributionSelector{ID: `*`}, CascadeDelete: $4}
	}
	| DROP SHARD any_id opt_cascade
	{
		$$ = &Drop{Element: &ShardSelector{ID: $3}, CascadeDelete: $4}
	}
	| DROP TASK GROUP any_id
	{
		$$ = &Drop{Element: &TaskGroupSelector{ ID: $4 }}
	}
	| DROP SEQUENCE any_id
	{
		$$ = &Drop{Element: &SequenceSelector{Name: $3}}
	}
	| DROP REFERENCE table_or_relation any_id
	{
		$$ = &Drop{
			Element: &ReferenceRelationSelector{
				ID: $4,
			},
		}
	}

add_stmt:
	// TODO: drop
	ADD distribution_define_stmt
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
	} |
	ADD shard_define_stmt
	{
		$$ = &Create{Element: $2}
	}

trace_stmt:
	START TRACE ALL MESSAGES
	{
		$$ = &TraceStmt{All: true}
	} | 
	START TRACE CLIENT any_uint {
		$$ = &TraceStmt {
			Client: $4,
		}
	}

stoptrace_stmt:
	STOP TRACE MESSAGES
	{
		$$ = &StopTraceStmt{}
	}

alter_stmt:
	ALTER distribution_alter_stmt
	{
		$$ = &Alter{Element: $2}
	}

distribution_alter_stmt:
	distribution_select_stmt relation_attach_stmt
	{
		$$ = &AlterDistribution{
			Distribution: $1,
			Element: &AttachRelation{
				Relations:     $2,
			},
		}
	} |
	distribution_select_stmt DETACH RELATION qualified_name
	{
		$$ = &AlterDistribution{
			Distribution: $1,
			Element: &DetachRelation{
				RelationName: $4,
			},
		}
	} |
	distribution_select_stmt relation_alter_stmt_v2
	{
		$$ = &AlterDistribution{
			Distribution: $1,
			Element: $2,
		}
	} |
	distribution_select_stmt ADD DEFAULT SHARD any_id
	{
		$$ = &AlterDistribution{
			Distribution: $1,
			Element: &AlterDefaultShard{
				Shard: $5,
			},
		}
	} |
	distribution_select_stmt DROP DEFAULT SHARD
	{
		$$ = &AlterDistribution{
			Distribution: $1,
			Element: &DropDefaultShard{},
		}
	}


distribution_key_argument_list: 
    distribution_key_argument_list TCOMMA distribution_key_entry
    {
      $$ = append($1, $3)
    } | distribution_key_entry {
      $$ = []DistributionKeyEntry {
		  $1,
	  }
    } 

typed_col_ref:
	any_id col_types_elem {
		$$ = TypedColRef{
			Column: $1,
			Type: $2,
		}
	}


routing_expr_column_list:
	typed_col_ref {
		$$ = []TypedColRef{ $1 }
	} | routing_expr_column_list TCOMMA typed_col_ref {
		$$ = append($1, $3)
	}

routing_expr:
	hash_function_name TOPENSQBR routing_expr_column_list TCLOSESQBR {
		$$ = DistributionKeyEntry{
			HashFunction: $1,
			Expr: $3,
		}
	}

distribution_key_entry:
	any_id opt_hash_function_clause
	{
		$$ = DistributionKeyEntry {
			Column: $1,
			HashFunction: $2,
		}
	} | routing_expr {
		$$ = $1
	}

distributed_relation_def:
	RELATION qualified_name DISTRIBUTION KEY distribution_key_argument_list opt_auto_increment opt_schema_name
	{
		if 	len($2.SchemaName)>0 && len($7)>0 {
			yylex.Error("it is forbidden to use both a qualified relation name and the keyword SCHEMA")
			return 1
		} else if len($2.SchemaName)>0 {
			$$ = &DistributedRelation{
				Name: 	 $2.RelationName,
				DistributionKey: $5,
				AutoIncrementEntries: $6,
				SchemaName: $2.SchemaName,
			}
		} else {
			$$ = &DistributedRelation{
				Name: 	 $2.RelationName,
				DistributionKey: $5,
				AutoIncrementEntries: $6,
				SchemaName: $7,
			}
		}
	} 
	| RELATION qualified_name TOPENBR distribution_key_argument_list opt_auto_increment opt_schema_name TCLOSEBR
	{
		if 	len($2.SchemaName)>0 && len($7)>0 {
			yylex.Error("it is forbidden to use both a qualified relation name and the keyword SCHEMA")
			return 1
		} else if len($2.SchemaName)>0 {
			$$ = &DistributedRelation{
				Name: 	 $2.RelationName,
				DistributionKey: $4,
				AutoIncrementEntries: $5,
				SchemaName: $2.SchemaName,
			}
		} else {
			$$ = &DistributedRelation{
				Name: 	 $2.RelationName,
				DistributionKey: $4,
				AutoIncrementEntries: $5,
				SchemaName: $6,
			}
		}
	} 

opt_auto_increment:
    AUTO INCREMENT auto_inc_argument_list {
        $$ = $3
    } | /* EMPTY */ {
        $$ = nil
    }

auto_inc_argument_list: 
    auto_inc_argument_list TCOMMA auto_increment_entry
    {
      $$ = append($1, $3)
    } | auto_increment_entry {
      $$ = []*AutoIncrementEntry {
		  $1,
	  }
    } 

auto_increment_entry:
	any_id opt_auto_increment_start_clause
	{
		$$ = &AutoIncrementEntry {
			Column: $1,
			Start: uint64($2),
		}
	}

opt_auto_increment_start_clause:
	START any_uint
	{
		$$ = $2
	} | /* EMPTY */ {
		$$ = 0
	}

opt_schema_name:
	SCHEMA any_id {
		$$ = $2
	} | /* EMPTY */ {
		$$ = ""
	}

distributed_relation_list_def:
	distributed_relation_def {
		$$ = []*DistributedRelation{$1}
	} | distributed_relation_list_def distributed_relation_def {
		$$  = append($1, $2)
	}

relation_attach_stmt:
	ATTACH distributed_relation_list_def {
		$$ = $2
	}

relation_alter_stmt_v2:
	ALTER RELATION qualified_name DISTRIBUTION KEY distribution_key_argument_list {
		$$ = &AlterRelationV2{
			RelationName: $3.RelationName,
			Element: &AlterRelationDistributionKey{
				DistributionKey: $6,
			},
		}
	} |
	ALTER RELATION qualified_name SCHEMA any_id {
		$$ = &AlterRelationV2{
			RelationName: $3.RelationName,
			Element: &AlterRelationSchema {
				SchemaName: $5,
			},
		}
	}

opt_distributed:
	DISTRIBUTED | {} /* nothing */

table_or_relation:
	TABLE {} | RELATION {}

tables_or_relations:
	TABLES {} | RELATIONS {}


opt_on_shards:
	ON any_id_list { $$ = $2 } | 
	ON SHARDS any_id_list { $$ = $3 } | /* nothing */ {}

create_stmt:
	CREATE distribution_define_stmt
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
	}
	|
	CREATE shard_define_stmt
	{
		$$ = &Create{Element: $2}
	}
	|
	CREATE REFERENCE table_or_relation qualified_name opt_auto_increment opt_on_shards
	{
		$$ = &Create{
			Element: &ReferenceRelationDefinition{
				TableName: $4,
                AutoIncrementEntries: $5,
				ShardIds: $6,
			},
		}
	}

opt_distribution_selector:
	FOR DISTRIBUTION any_id {
		$$ = &DistributionSelector{ID: $3}
	} |
	IN any_id {
		$$ = &DistributionSelector{ID: $2}
	} |  /* empty means default */ {
		$$ = &DistributionSelector{ID: "default"}
	}

create_distributed_relation_stmt:
	CREATE opt_distributed distributed_relation_def opt_distribution_selector
	{
		$$ = &Alter{
			Element: &AlterDistribution{
				Distribution: 	$4,
				Element: &AttachRelation{
					Relations:      []*DistributedRelation{$3},
				},
			},
		}
	}


opt_asc_desc: ASC							{ $$ = &SortByAsc{} }
			| DESC							{ $$ = &SortByDesc{} }
			| /*EMPTY*/						{ $$ = &SortByDefault{} }

order_clause:
    ORDER BY ColRef opt_asc_desc 
	{
		$$ = &Order{Col:$3, OptAscDesc:$4}
	} 
	| /* empty */    {$$ = OrderClause(nil)}


group_clause:
	GROUP BY ColRef_list
	{
		$$ = GroupBy{Col: $3}
	}
	| /* empty */	 {$$ = GroupByClauseEmpty{}}


show_stmt:
	SHOW show_statement_type where_clause group_clause order_clause
	{
		$$ = &Show{Cmd: $2, Where: $3, GroupBy: $4, Order: $5}
	}
	
lock_stmt:
	LOCK key_range_stmt
	{
		$$ = &Lock{KeyRangeID: $2.KeyRangeID}
	}
	// or lock something else


distribution_define_stmt:
	DISTRIBUTION any_id opt_col_types opt_default_shard
	{
		$$ = &DistributionDefinition{
			ID: $2,
			ColTypes: $3,
			DefaultShard: $4,
		}
	}

opt_col_types:
	COLUMN TYPES col_types_list {
		$$ = $3
	} | { 
		/* empty column types should be prohibited */
		$$ = nil 
	}

col_types_list:
	col_types_list TCOMMA col_types_elem {
		$$ = append($1, $3)
	} | col_types_elem {
		$$ = []string {
			$1,
		}
	}

col_types_elem:
	VARCHAR {
		$$ = qdb.ColumnTypeVarchar
	} | VARCHAR HASH {
		$$ = qdb.ColumnTypeVarcharHashed
	} | INTEGER {
		$$ = qdb.ColumnTypeInteger
	} | INT {
		$$ = qdb.ColumnTypeInteger
	} | INTEGER HASH {
		$$ = qdb.ColumnTypeUinteger
	} | INT HASH {
		$$ = qdb.ColumnTypeUinteger
	} | UUID {
		$$ = qdb.ColumnTypeUUID
	}

opt_default_shard:
	DEFAULT SHARD any_id {
		$$ = $3
	} | /* EMPTY */ {
		$$ = ""
	}

sharding_rule_define_stmt:
	SHARDING RULE any_id sharding_rule_table_clause sharding_rule_argument_list opt_distribution_selector
	{
	}
	|
	SHARDING RULE sharding_rule_table_clause sharding_rule_argument_list opt_distribution_selector
	{
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
	sharding_rule_column_clause opt_hash_function_clause
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


hash_function_name:
	IDENTITY {
		$$ = "identity"
	} | MURMUR {
		$$ = "murmur"
	} | CITY {
		$$ = "city"
	}

opt_function:
	FUNCTION {} | {}

hash_function_clause:
	HASH opt_function hash_function_name
	{
		$$ = $3
	}

opt_hash_function_clause:
	hash_function_clause {
		$$ = $1
	} | /* EMPTY */ {
		$$ = ""
	}

key_range_bound_elem:
	SCONST
	{
		$$ = []byte($1)
	} | 
	IDENT
	{
		$$ = []byte($1)
	} | ICONST {
		if $1 > uint(math.MaxInt64) {
			yylex.Error(SIGNED_INT_RANGE_ERROR)
			return 1
		} else {
			buf := make([]byte, binary.MaxVarintLen64)
			binary.PutVarint(buf, int64($1))
			$$ = buf
		}
	} | TMINUS ICONST {
		if $2 > uint(-math.MinInt64) {
			yylex.Error(SIGNED_INT_RANGE_ERROR)
			return 1
		} else {
			buf := make([]byte, binary.MaxVarintLen64)
			binary.PutVarint(buf, int64(-$2))
			$$ = buf
		}
	}

key_range_bound:
	key_range_bound_elem { 
		$$ = &KeyRangeBound{
			Pivots: [][]byte{
				$1,
			},
		}
	} 
	| key_range_bound TCOMMA key_range_bound_elem {
		$$ = &KeyRangeBound{
			Pivots: append($1.Pivots, $3),
		}
	}


key_range_define_stmt:
	KEY RANGE any_id FROM key_range_bound ROUTE TO shard_id opt_distribution_selector
	{
		$$ = &KeyRangeDefinition{
			KeyRangeID: $3,
			LowerBound: $5,
			ShardID: $8,
			Distribution: $9,
		}
	}
	| KEY RANGE FROM key_range_bound ROUTE TO any_id opt_distribution_selector
	{
		str, err := randomHex(6)
		if err != nil {
			panic(err)
		}
		$$ = &KeyRangeDefinition{
			LowerBound: $4,
			ShardID: $7,
			Distribution: $8,
			KeyRangeID: "kr"+str,
		}
	}

shard_define_stmt:
	SHARD any_id WITH HOSTS any_id_list
	{
		$$ = &ShardDefinition{Id: $2, Hosts: $5}
	}
	|
	SHARD WITH HOSTS any_id_list
	{
		str, err := randomHex(6)
		if err != nil {
			panic(err)
		}
		$$ = &ShardDefinition{Id: "shard" + str, Hosts: $4}
	}

any_id_list:
	any_val
	{
		$$ = []string{$1}
	}
	|
	any_id_list TCOMMA any_val
	{
		$$ = append($1, $3)
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

distribution_select_stmt:
	DISTRIBUTION any_id
	{
		$$ = &DistributionSelector{ID: $2}
	}

split_key_range_stmt:
	SPLIT key_range_stmt FROM any_id BY key_range_bound
	{
		$$ = &SplitKeyRange{KeyRangeID: $2.KeyRangeID, KeyRangeFromID: $4, Border: $6}
	}

kill_stmt:
	KILL kill_statement_type any_uint
	{
		$$ = &Kill{Cmd: $2, Target: $3}
	}


move_key_range_stmt:
	MOVE key_range_stmt TO any_id
	{
		$$ = &MoveKeyRange{KeyRangeID: $2.KeyRangeID, DestShardID: $4}
	}

redistribute_key_range_stmt:
	REDISTRIBUTE key_range_stmt TO any_id opt_batch_size
	{
		$$ = &RedistributeKeyRange{KeyRangeID: $2.KeyRangeID, DestShardID: $4, BatchSize: $5, Check: true, Apply: true}
	} | REDISTRIBUTE key_range_stmt TO any_id opt_batch_size CHECK {
		$$ = &RedistributeKeyRange{KeyRangeID: $2.KeyRangeID, DestShardID: $4, BatchSize: $5, Check: true}
	} | REDISTRIBUTE key_range_stmt TO any_id opt_batch_size APPLY {
		$$ = &RedistributeKeyRange{KeyRangeID: $2.KeyRangeID, DestShardID: $4, BatchSize: $5, Apply: true}
	}

opt_batch_size: BATCH SIZE any_uint			{ $$ = int($3) }
			| /*EMPTY*/						{ $$ = -1 }

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

invalidate_stmt:
	INVALIDATE CACHE
	{
		$$ = &Invalidate{
			Target: SchemaCacheInvalidateTarget,
		}
	} | INVALIDATE SCHEMA CACHE
	{
		$$ =  &Invalidate{
			Target: SchemaCacheInvalidateTarget,
		}
	} | INVALIDATE BACKENDS
	{
		$$ = &Invalidate{
			Target: BackendConnectionsInvalidateTarget,
		}
	}

sync_reference_tables_stmt:
	SYNC REFERENCE tables_or_relations ON any_id
	{
		$$ = &SyncReferenceTables {
			ShardID: $5,
			RelationSelector: "*",
		}
	} | SYNC REFERENCE table_or_relation any_id ON any_id
	{
		$$ = &SyncReferenceTables {
			ShardID: $6,
			RelationSelector: $4,
		}
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

// move tasks

retry_move_task_group:
	RETRY MOVE TASK GROUP any_id
	{
		$$ = &RetryMoveTaskGroup{ ID: $5 }
	}
	|
	RETRY TASK GROUP any_id
	{
		$$ = &RetryMoveTaskGroup{ ID: $4 }
	}
	

stop_move_task_group:
	STOP MOVE TASK GROUP any_id
	{
		$$ = &StopMoveTaskGroup{ ID: $5 }
	}
	|
	STOP TASK GROUP any_id
	{
		$$ = &StopMoveTaskGroup{ ID: $4 }
	} 
%%

