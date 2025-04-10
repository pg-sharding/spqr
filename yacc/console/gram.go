// Code generated by goyacc -o gram.go -p yy gram.y. DO NOT EDIT.

//line gram.y:3
package spqrparser

import __yyfmt__ "fmt"

//line gram.y:3

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
)

func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

//line gram.y:25
type yySymType struct {
	yys      int
	str      string
	strlist  []string
	byte     byte
	bytes    []byte
	integer  int
	uinteger uint
	bool     bool
	empty    struct{}

	set       *Set
	statement Statement
	show      *Show

	drop   *Drop
	create *Create

	kill   *Kill
	lock   *Lock
	unlock *Unlock

	krbound *KeyRangeBound

	ds            *DistributionDefinition
	kr            *KeyRangeDefinition
	shard         *ShardDefinition
	sharding_rule *ShardingRuleDefinition

	register_router   *RegisterRouter
	unregister_router *UnregisterRouter

	split *SplitKeyRange
	move  *MoveKeyRange
	unite *UniteKeyRange

	redistribute *RedistributeKeyRange

	invalidate_cache *InvalidateCache

	shutdown *Shutdown
	listen   *Listen

	trace     *TraceStmt
	stoptrace *StopTraceStmt

	distribution *DistributionDefinition

	alter                *Alter
	alter_distribution   *AlterDistribution
	distributed_relation *DistributedRelation

	relations    []*DistributedRelation
	entrieslist  []ShardingRuleEntry
	dEntrieslist []DistributionKeyEntry

	shruleEntry ShardingRuleEntry

	distrKeyEntry DistributionKeyEntry

	sharding_rule_selector *ShardingRuleSelector
	key_range_selector     *KeyRangeSelector
	distribution_selector  *DistributionSelector

	colref ColumnRef
	where  WhereClauseNode

	order_clause OrderClause
	opt_asc_desc OptAscDesc

	group_clause GroupByClause

	opt_batch_size int
}

const IDENT = 57346
const COMMAND = 57347
const SHOW = 57348
const KILL = 57349
const WHERE = 57350
const OR = 57351
const AND = 57352
const TEQ = 57353
const TCOMMA = 57354
const SCONST = 57355
const ICONST = 57356
const TSEMICOLON = 57357
const TOPENBR = 57358
const TCLOSEBR = 57359
const SHUTDOWN = 57360
const LISTEN = 57361
const REGISTER = 57362
const UNREGISTER = 57363
const ROUTER = 57364
const ROUTE = 57365
const CREATE = 57366
const ADD = 57367
const DROP = 57368
const LOCK = 57369
const UNLOCK = 57370
const SPLIT = 57371
const MOVE = 57372
const COMPOSE = 57373
const SET = 57374
const CASCADE = 57375
const ATTACH = 57376
const ALTER = 57377
const DETACH = 57378
const REDISTRIBUTE = 57379
const REFERENCE = 57380
const CHECK = 57381
const APPLY = 57382
const SHARDING = 57383
const COLUMN = 57384
const TABLE = 57385
const HASH = 57386
const FUNCTION = 57387
const KEY = 57388
const RANGE = 57389
const DISTRIBUTION = 57390
const RELATION = 57391
const REPLICATED = 57392
const AUTO = 57393
const INCREMENT = 57394
const SEQUENCE = 57395
const SHARDS = 57396
const KEY_RANGES = 57397
const ROUTERS = 57398
const SHARD = 57399
const HOST = 57400
const SHARDING_RULES = 57401
const RULE = 57402
const COLUMNS = 57403
const VERSION = 57404
const HOSTS = 57405
const SEQUENCES = 57406
const BY = 57407
const FROM = 57408
const TO = 57409
const WITH = 57410
const UNITE = 57411
const ALL = 57412
const ADDRESS = 57413
const FOR = 57414
const CLIENT = 57415
const BATCH = 57416
const SIZE = 57417
const INVALIDATE = 57418
const CACHE = 57419
const IDENTITY = 57420
const MURMUR = 57421
const CITY = 57422
const START = 57423
const STOP = 57424
const TRACE = 57425
const MESSAGES = 57426
const TASK = 57427
const GROUP = 57428
const VARCHAR = 57429
const INTEGER = 57430
const INT = 57431
const TYPES = 57432
const OP = 57433
const ASC = 57434
const DESC = 57435
const ORDER = 57436

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"IDENT",
	"COMMAND",
	"SHOW",
	"KILL",
	"WHERE",
	"OR",
	"AND",
	"TEQ",
	"TCOMMA",
	"SCONST",
	"ICONST",
	"TSEMICOLON",
	"TOPENBR",
	"TCLOSEBR",
	"SHUTDOWN",
	"LISTEN",
	"REGISTER",
	"UNREGISTER",
	"ROUTER",
	"ROUTE",
	"CREATE",
	"ADD",
	"DROP",
	"LOCK",
	"UNLOCK",
	"SPLIT",
	"MOVE",
	"COMPOSE",
	"SET",
	"CASCADE",
	"ATTACH",
	"ALTER",
	"DETACH",
	"REDISTRIBUTE",
	"REFERENCE",
	"CHECK",
	"APPLY",
	"SHARDING",
	"COLUMN",
	"TABLE",
	"HASH",
	"FUNCTION",
	"KEY",
	"RANGE",
	"DISTRIBUTION",
	"RELATION",
	"REPLICATED",
	"AUTO",
	"INCREMENT",
	"SEQUENCE",
	"SHARDS",
	"KEY_RANGES",
	"ROUTERS",
	"SHARD",
	"HOST",
	"SHARDING_RULES",
	"RULE",
	"COLUMNS",
	"VERSION",
	"HOSTS",
	"SEQUENCES",
	"BY",
	"FROM",
	"TO",
	"WITH",
	"UNITE",
	"ALL",
	"ADDRESS",
	"FOR",
	"CLIENT",
	"BATCH",
	"SIZE",
	"INVALIDATE",
	"CACHE",
	"IDENTITY",
	"MURMUR",
	"CITY",
	"START",
	"STOP",
	"TRACE",
	"MESSAGES",
	"TASK",
	"GROUP",
	"VARCHAR",
	"INTEGER",
	"INT",
	"TYPES",
	"OP",
	"ASC",
	"DESC",
	"ORDER",
}

var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

//line gram.y:968

//line yacctab:1
var yyExca = [...]int8{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 297

var yyAct = [...]int16{
	150, 256, 203, 177, 198, 172, 206, 171, 149, 170,
	163, 169, 175, 147, 162, 131, 158, 104, 253, 254,
	160, 180, 29, 30, 199, 200, 201, 146, 109, 138,
	101, 58, 57, 90, 32, 31, 37, 38, 224, 77,
	23, 22, 26, 27, 28, 33, 34, 192, 91, 76,
	205, 39, 96, 35, 246, 247, 248, 99, 249, 165,
	100, 155, 92, 92, 92, 107, 108, 62, 110, 92,
	92, 135, 60, 121, 64, 236, 68, 92, 166, 67,
	115, 117, 120, 65, 168, 36, 122, 123, 119, 205,
	107, 118, 40, 216, 190, 130, 133, 24, 25, 137,
	181, 174, 136, 141, 143, 215, 139, 103, 94, 112,
	159, 66, 165, 141, 178, 261, 156, 232, 75, 151,
	152, 153, 154, 225, 144, 89, 178, 68, 142, 140,
	124, 166, 111, 167, 97, 106, 93, 56, 102, 134,
	49, 95, 242, 49, 176, 50, 161, 47, 50, 48,
	47, 70, 48, 233, 178, 207, 51, 194, 230, 51,
	196, 229, 186, 193, 228, 92, 208, 209, 132, 98,
	129, 222, 223, 204, 195, 63, 202, 127, 105, 126,
	210, 211, 176, 59, 46, 45, 211, 213, 86, 85,
	217, 42, 234, 116, 251, 220, 218, 212, 92, 44,
	226, 183, 221, 43, 132, 231, 185, 184, 55, 54,
	148, 69, 71, 204, 219, 88, 239, 81, 82, 83,
	84, 235, 237, 53, 211, 240, 79, 52, 214, 79,
	241, 227, 243, 244, 188, 78, 80, 250, 78, 173,
	183, 189, 114, 257, 92, 185, 184, 73, 41, 1,
	258, 191, 260, 259, 19, 18, 17, 16, 15, 262,
	14, 264, 257, 265, 263, 12, 13, 8, 9, 145,
	252, 179, 128, 197, 157, 125, 21, 87, 20, 245,
	164, 238, 255, 6, 5, 4, 3, 7, 11, 10,
	74, 72, 61, 2, 187, 182, 113,
}

var yyPact = [...]int16{
	16, -1000, 176, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 102, 99, -51, -52, 26, 105, 105, 243,
	45, 222, -1000, 105, 105, 105, 105, 167, 166, 77,
	-44, -1000, -1000, -1000, -1000, -1000, -1000, 240, 88, 48,
	94, 66, -1000, -1000, -1000, -1000, 126, -13, -54, -1000,
	91, -1000, 47, 145, 65, 240, -58, 240, 84, -1000,
	62, -1000, 234, -1000, 179, 179, -1000, -1000, -1000, -1000,
	-1000, 25, 21, 15, 5, 240, 60, -1000, 143, 240,
	-1000, 128, -1000, -1000, 161, 73, 3, 39, 240, -55,
	179, -1000, 59, 58, -1000, -1000, 145, -1000, -1000, -1000,
	-1000, -1000, 240, -59, 194, -1000, -1000, -1000, 240, 240,
	240, 240, -10, -1000, -1000, -1000, 67, 61, -1000, -70,
	125, 70, 240, 18, 225, 38, 222, 63, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -73, 35, 236, 194, 230,
	-1000, 29, -1000, -27, -1000, 222, 240, 61, -1000, 240,
	-63, 70, 17, -1000, 111, 240, 240, -1000, 225, 174,
	-1000, -1000, -1000, -1000, 222, 216, -1000, -1000, 53, -1000,
	28, 240, 194, -1000, -1000, -1000, 197, 222, -1000, -1000,
	225, 132, -37, -1000, -1000, -1000, 75, 219, -1000, 120,
	117, 114, 17, -1000, -1000, 69, -1000, 108, -1000, -1000,
	169, 225, 8, 216, 222, 240, 240, -1000, 236, -1000,
	-1000, 212, -1000, -1000, 179, 96, -1000, -63, -1000, -1000,
	-1000, -1000, 240, -24, -9, -1000, 240, -1000, 182, -1000,
	-74, -1000, 240, -1000, -1000, -1000, -1000, -1000, -1000, 240,
	-22, 240, -1000, -1000, -1000, 103, -1000, 111, -22, -1000,
	-1000, 240, -1000, -1000, -1000, -1000,
}

var yyPgo = [...]int16{
	0, 296, 13, 9, 11, 295, 294, 8, 7, 0,
	5, 293, 292, 183, 175, 291, 290, 289, 288, 287,
	286, 285, 284, 283, 203, 199, 185, 184, 14, 282,
	10, 3, 281, 1, 15, 280, 6, 279, 2, 278,
	277, 276, 275, 274, 16, 273, 272, 12, 4, 17,
	271, 270, 269, 268, 267, 266, 265, 260, 258, 257,
	256, 255, 254, 251, 249, 248,
}

var yyR1 = [...]int8{
	0, 64, 65, 65, 11, 11, 11, 11, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
	11, 11, 11, 10, 8, 8, 8, 9, 5, 5,
	5, 6, 6, 7, 2, 2, 2, 1, 1, 15,
	16, 49, 49, 19, 19, 19, 19, 19, 19, 19,
	19, 19, 20, 20, 20, 20, 22, 22, 23, 39,
	40, 40, 29, 29, 33, 44, 44, 31, 31, 32,
	32, 43, 43, 42, 21, 21, 21, 21, 21, 51,
	51, 51, 50, 50, 52, 52, 17, 54, 24, 24,
	46, 46, 45, 45, 48, 48, 48, 48, 48, 48,
	25, 25, 28, 28, 30, 34, 34, 35, 35, 37,
	37, 37, 36, 36, 38, 38, 3, 3, 4, 4,
	26, 26, 27, 27, 47, 47, 53, 12, 13, 14,
	14, 57, 18, 18, 58, 59, 59, 59, 63, 63,
	60, 56, 55, 41, 61, 62, 62,
}

var yyR2 = [...]int8{
	0, 2, 0, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 3, 3, 3, 0, 2, 1,
	1, 1, 0, 2, 4, 2, 4, 3, 4, 3,
	3, 3, 2, 2, 2, 2, 4, 4, 3, 2,
	2, 4, 3, 1, 2, 6, 3, 3, 0, 1,
	3, 1, 2, 2, 2, 2, 2, 2, 5, 1,
	1, 0, 4, 0, 3, 0, 5, 2, 3, 2,
	3, 0, 3, 1, 1, 2, 1, 1, 2, 2,
	6, 5, 1, 2, 2, 2, 0, 2, 2, 1,
	1, 1, 3, 0, 3, 0, 1, 1, 1, 3,
	9, 8, 5, 4, 1, 3, 2, 3, 3, 2,
	2, 6, 3, 3, 4, 5, 6, 6, 3, 0,
	4, 2, 1, 2, 5, 3, 3,
}

var yyChk = [...]int16{
	-1000, -64, -11, -20, -21, -22, -23, -19, -54, -53,
	-17, -18, -56, -55, -57, -58, -59, -60, -61, -62,
	-39, -41, 25, 24, 81, 82, 26, 27, 28, 6,
	7, 19, 18, 29, 30, 37, 69, 20, 21, 35,
	76, -65, 15, -24, -25, -26, -27, 48, 50, 41,
	46, 57, -24, -25, -26, -27, 38, 83, 83, -13,
	46, -12, 41, -14, 48, 57, 85, 53, 50, -13,
	46, -13, -15, 4, -16, 73, 4, -8, 13, 4,
	14, -13, -13, -13, -13, 22, 22, -40, -14, 48,
	77, -9, 4, 48, 60, 47, -9, 68, 43, 70,
	73, 84, 47, 60, -49, 33, 70, -9, -9, 86,
	-9, 48, 47, -1, 8, -10, 14, -10, 66, 67,
	67, 68, -9, -9, 70, -42, 36, 34, -46, 42,
	-9, -34, 43, -9, 66, 68, 63, -9, 84, -10,
	70, -9, 70, -9, -49, -52, 86, -2, 16, -7,
	-9, -9, -9, -9, -9, 71, 49, -43, -44, 49,
	90, -34, -28, -30, -35, 42, 61, -9, 66, -4,
	-3, -8, -10, 14, 63, -47, -8, -31, 51, -50,
	94, 65, -5, 4, 10, 9, -2, -6, 4, 11,
	65, -63, 74, -8, -9, -44, -9, -45, -48, 87,
	88, 89, -28, -38, -30, 72, -36, 44, -9, -9,
	-4, 12, 23, -47, 12, 52, 65, -7, -2, 17,
	-8, -4, 39, 40, 75, 48, -31, 12, 44, 44,
	44, -38, 48, 45, 23, -3, 67, -8, -32, -9,
	-7, -10, 46, -48, -9, -37, 78, 79, 80, 67,
	-9, 12, -51, 92, 93, -29, -33, -9, -9, -38,
	-9, 12, -31, -36, -38, -33,
}

var yyDef = [...]int16{
	0, -2, 2, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
	21, 22, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 142, 0, 0, 0, 0, 0, 0, 0,
	0, 1, 3, 52, 53, 54, 55, 0, 0, 0,
	0, 0, 74, 75, 76, 77, 0, 0, 0, 43,
	0, 45, 0, 42, 0, 0, 0, 0, 0, 87,
	0, 126, 37, 39, 0, 0, 40, 141, 24, 25,
	26, 0, 0, 0, 0, 0, 0, 59, 0, 0,
	143, 91, 27, 89, 106, 0, 0, 0, 0, 0,
	0, 58, 0, 0, 47, 41, 42, 129, 49, 50,
	51, 130, 0, 85, 0, 132, 23, 133, 0, 0,
	0, 0, 0, 145, 146, 60, 0, 0, 88, 0,
	106, 0, 0, 0, 0, 0, 0, 68, 56, 57,
	44, 128, 46, 127, 48, 83, 0, 38, 0, 0,
	33, 0, 134, 139, 140, 0, 0, 73, 71, 0,
	0, 0, 115, 102, 113, 0, 0, 105, 0, 0,
	118, 116, 117, 23, 0, 123, 124, 78, 0, 86,
	0, 0, 0, 28, 29, 30, 0, 0, 31, 32,
	0, 135, 0, 144, 61, 72, 68, 90, 93, 94,
	96, 97, 115, 101, 103, 0, 104, 0, 107, 108,
	0, 0, 0, 122, 0, 0, 0, 84, 36, 34,
	35, 131, 136, 137, 0, 0, 66, 0, 95, 98,
	99, 100, 0, 0, 0, 119, 0, 125, 67, 69,
	81, 138, 0, 92, 114, 112, 109, 110, 111, 0,
	115, 0, 82, 79, 80, 68, 63, 113, 115, 121,
	70, 0, 65, 64, 120, 62,
}

var yyTok1 = [...]int8{
	1,
}

var yyTok2 = [...]int8{
	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
	62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
	72, 73, 74, 75, 76, 77, 78, 79, 80, 81,
	82, 83, 84, 85, 86, 87, 88, 89, 90, 91,
	92, 93, 94,
}

var yyTok3 = [...]int8{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

//line yaccpar:1

/*	parser for yacc output	*/

var (
	yyDebug        = 0
	yyErrorVerbose = false
)

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyParser interface {
	Parse(yyLexer) int
	Lookahead() int
}

type yyParserImpl struct {
	lval  yySymType
	stack [yyInitialStackSize]yySymType
	char  int
}

func (p *yyParserImpl) Lookahead() int {
	return p.char
}

func yyNewParser() yyParser {
	return &yyParserImpl{}
}

const yyFlag = -1000

func yyTokname(c int) string {
	if c >= 1 && c-1 < len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yyErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !yyErrorVerbose {
		return "syntax error"
	}

	for _, e := range yyErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + yyTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := int(yyPact[state])
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && int(yyChk[int(yyAct[n])]) == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || int(yyExca[i+1]) != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := int(yyExca[i])
			if tok < TOKSTART || yyExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if yyExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += yyTokname(tok)
	}
	return res
}

func yylex1(lex yyLexer, lval *yySymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = int(yyTok1[0])
		goto out
	}
	if char < len(yyTok1) {
		token = int(yyTok1[char])
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = int(yyTok2[char-yyPrivate])
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = int(yyTok3[i+0])
		if token == char {
			token = int(yyTok3[i+1])
			goto out
		}
	}

out:
	if token == 0 {
		token = int(yyTok2[1]) /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(token), uint(char))
	}
	return char, token
}

func yyParse(yylex yyLexer) int {
	return yyNewParser().Parse(yylex)
}

func (yyrcvr *yyParserImpl) Parse(yylex yyLexer) int {
	var yyn int
	var yyVAL yySymType
	var yyDollar []yySymType
	_ = yyDollar // silence set and not used
	yyS := yyrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yyrcvr.char = -1
	yytoken := -1 // yyrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yyrcvr.char = -1
		yytoken = -1
	}()
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yytoken), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = int(yyPact[yystate])
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yyrcvr.char < 0 {
		yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = int(yyAct[yyn])
	if int(yyChk[yyn]) == yytoken { /* valid shift */
		yyrcvr.char = -1
		yytoken = -1
		yyVAL = yyrcvr.lval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = int(yyDef[yystate])
	if yyn == -2 {
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && int(yyExca[xi+1]) == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = int(yyExca[xi+0])
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = int(yyExca[xi+1])
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error(yyErrorMessage(yystate, yytoken))
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yytoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = int(yyPact[yyS[yyp].yys]) + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = int(yyAct[yyn]) /* simulate a shift of "error" */
					if int(yyChk[yystate]) == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yytoken))
			}
			if yytoken == yyEofCode {
				goto ret1
			}
			yyrcvr.char = -1
			yytoken = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= int(yyR2[yyn])
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = int(yyR1[yyn])
	yyg := int(yyPgo[yyn])
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = int(yyAct[yyg])
	} else {
		yystate = int(yyAct[yyj])
		if int(yyChk[yystate]) != -yyn {
			yystate = int(yyAct[yyg])
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 2:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:245
		{
		}
	case 3:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:246
		{
		}
	case 4:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:251
		{
			setParseTree(yylex, yyDollar[1].create)
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:255
		{
			setParseTree(yylex, yyDollar[1].create)
		}
	case 6:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:259
		{
			setParseTree(yylex, yyDollar[1].trace)
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:263
		{
			setParseTree(yylex, yyDollar[1].stoptrace)
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:267
		{
			setParseTree(yylex, yyDollar[1].drop)
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:271
		{
			setParseTree(yylex, yyDollar[1].lock)
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:275
		{
			setParseTree(yylex, yyDollar[1].unlock)
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:279
		{
			setParseTree(yylex, yyDollar[1].show)
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:283
		{
			setParseTree(yylex, yyDollar[1].kill)
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:287
		{
			setParseTree(yylex, yyDollar[1].listen)
		}
	case 14:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:291
		{
			setParseTree(yylex, yyDollar[1].shutdown)
		}
	case 15:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:295
		{
			setParseTree(yylex, yyDollar[1].split)
		}
	case 16:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:299
		{
			setParseTree(yylex, yyDollar[1].move)
		}
	case 17:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:303
		{
			setParseTree(yylex, yyDollar[1].redistribute)
		}
	case 18:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:307
		{
			setParseTree(yylex, yyDollar[1].unite)
		}
	case 19:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:311
		{
			setParseTree(yylex, yyDollar[1].register_router)
		}
	case 20:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:315
		{
			setParseTree(yylex, yyDollar[1].unregister_router)
		}
	case 21:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:319
		{
			setParseTree(yylex, yyDollar[1].alter)
		}
	case 22:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:323
		{
			setParseTree(yylex, yyDollar[1].invalidate_cache)
		}
	case 23:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:328
		{
			yyVAL.uinteger = uint(yyDollar[1].uinteger)
		}
	case 24:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:333
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 25:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:337
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 26:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:339
		{
			yyVAL.str = strconv.Itoa(int(yyDollar[1].uinteger))
		}
	case 27:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:344
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 28:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:350
		{
			yyVAL.str = yyDollar[1].str
		}
	case 29:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:352
		{
			yyVAL.str = "AND"
		}
	case 30:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:354
		{
			yyVAL.str = "OR"
		}
	case 31:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:359
		{
			yyVAL.str = yyDollar[1].str
		}
	case 32:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:361
		{
			yyVAL.str = "="
		}
	case 33:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:367
		{
			yyVAL.colref = ColumnRef{
				ColName: yyDollar[1].str,
			}
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:375
		{
			yyVAL.where = yyDollar[2].where
		}
	case 35:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:378
		{
			yyVAL.where = WhereClauseLeaf{
				ColRef: yyDollar[1].colref,
				Op:     yyDollar[2].str,
				Value:  yyDollar[3].str,
			}
		}
	case 36:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:386
		{
			yyVAL.where = WhereClauseOp{
				Op:    yyDollar[2].str,
				Left:  yyDollar[1].where,
				Right: yyDollar[3].where,
			}
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:396
		{
			yyVAL.where = WhereClauseEmpty{}
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:400
		{
			yyVAL.where = yyDollar[2].where
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:407
		{
			switch v := strings.ToLower(string(yyDollar[1].str)); v {
			case DatabasesStr, RoutersStr, PoolsStr, InstanceStr, ShardsStr, BackendConnectionsStr, KeyRangesStr, ShardingRules, ClientsStr, StatusStr, DistributionsStr, VersionStr, RelationsStr, TaskGroupStr, PreparedStatementsStr, QuantilesStr, SequencesStr:
				yyVAL.str = v
			default:
				yyVAL.str = UnsupportedStr
			}
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:418
		{
			switch v := string(yyDollar[1].str); v {
			case ClientStr:
				yyVAL.str = v
			default:
				yyVAL.str = UnsupportedStr
			}
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:428
		{
			yyVAL.bool = true
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:428
		{
			yyVAL.bool = false
		}
	case 43:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:432
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].key_range_selector}
		}
	case 44:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:436
		{
			yyVAL.drop = &Drop{Element: &KeyRangeSelector{KeyRangeID: `*`}}
		}
	case 45:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:440
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].sharding_rule_selector}
		}
	case 46:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:444
		{
			yyVAL.drop = &Drop{Element: &ShardingRuleSelector{ID: `*`}}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:448
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].distribution_selector, CascadeDelete: yyDollar[3].bool}
		}
	case 48:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:452
		{
			yyVAL.drop = &Drop{Element: &DistributionSelector{ID: `*`}, CascadeDelete: yyDollar[4].bool}
		}
	case 49:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:456
		{
			yyVAL.drop = &Drop{Element: &ShardSelector{ID: yyDollar[3].str}}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:460
		{
			yyVAL.drop = &Drop{Element: &TaskGroupSelector{}}
		}
	case 51:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:464
		{
			yyVAL.drop = &Drop{Element: &SequenceSelector{Name: yyDollar[3].str}}
		}
	case 52:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:471
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 53:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:476
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:481
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:485
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 56:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:491
		{
			yyVAL.trace = &TraceStmt{All: true}
		}
	case 57:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:494
		{
			yyVAL.trace = &TraceStmt{
				Client: yyDollar[4].uinteger,
			}
		}
	case 58:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:502
		{
			yyVAL.stoptrace = &StopTraceStmt{}
		}
	case 59:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:508
		{
			yyVAL.alter = &Alter{Element: yyDollar[2].alter_distribution}
		}
	case 60:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:514
		{
			yyVAL.alter_distribution = &AlterDistribution{
				Element: &AttachRelation{
					Distribution: yyDollar[1].distribution_selector,
					Relations:    yyDollar[2].relations,
				},
			}
		}
	case 61:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:523
		{
			yyVAL.alter_distribution = &AlterDistribution{
				Element: &DetachRelation{
					Distribution: yyDollar[1].distribution_selector,
					RelationName: yyDollar[4].str,
				},
			}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:535
		{
			yyVAL.dEntrieslist = append(yyDollar[1].dEntrieslist, yyDollar[3].distrKeyEntry)
		}
	case 63:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:537
		{
			yyVAL.dEntrieslist = []DistributionKeyEntry{
				yyDollar[1].distrKeyEntry,
			}
		}
	case 64:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:547
		{
			yyVAL.distrKeyEntry = DistributionKeyEntry{
				Column:       yyDollar[1].str,
				HashFunction: yyDollar[2].str,
			}
		}
	case 65:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:556
		{
			yyVAL.distributed_relation = &DistributedRelation{
				Name:                 yyDollar[2].str,
				DistributionKey:      yyDollar[5].dEntrieslist,
				AutoIncrementColumns: yyDollar[6].strlist,
			}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:564
		{
			yyVAL.distributed_relation = &DistributedRelation{
				Name:                 yyDollar[2].str,
				ReplicatedRelation:   true,
				AutoIncrementColumns: yyDollar[3].strlist,
			}
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:574
		{
			yyVAL.strlist = yyDollar[3].strlist
		}
	case 68:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:576
		{
			yyVAL.strlist = nil
		}
	case 69:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:581
		{
			yyVAL.strlist = []string{yyDollar[1].str}
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:583
		{
			yyVAL.strlist = append(yyDollar[1].strlist, yyDollar[3].str)
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:589
		{
			yyVAL.relations = []*DistributedRelation{yyDollar[1].distributed_relation}
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:591
		{
			yyVAL.relations = append(yyDollar[1].relations, yyDollar[2].distributed_relation)
		}
	case 73:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:596
		{
			yyVAL.relations = yyDollar[2].relations
		}
	case 74:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:602
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 75:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:607
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:612
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 77:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:617
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 78:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:622
		{
			yyVAL.create = &Create{
				Element: &ReferenceRelationDefinition{
					TableName:            yyDollar[4].str,
					AutoIncrementColumns: yyDollar[5].strlist,
				},
			}
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:632
		{
			yyVAL.opt_asc_desc = &SortByAsc{}
		}
	case 80:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:633
		{
			yyVAL.opt_asc_desc = &SortByDesc{}
		}
	case 81:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:634
		{
			yyVAL.opt_asc_desc = &SortByDefault{}
		}
	case 82:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:638
		{
			yyVAL.order_clause = &Order{Col: yyDollar[3].colref, OptAscDesc: yyDollar[4].opt_asc_desc}
		}
	case 83:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:641
		{
			yyVAL.order_clause = OrderClause(nil)
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:646
		{
			yyVAL.group_clause = GroupBy{Col: yyDollar[3].colref}
		}
	case 85:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:649
		{
			yyVAL.group_clause = GroupByClauseEmpty{}
		}
	case 86:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:654
		{
			yyVAL.show = &Show{Cmd: yyDollar[2].str, Where: yyDollar[3].where, GroupBy: yyDollar[4].group_clause, Order: yyDollar[5].order_clause}
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:659
		{
			yyVAL.lock = &Lock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:667
		{
			yyVAL.ds = &DistributionDefinition{
				ID:       yyDollar[2].str,
				ColTypes: yyDollar[3].strlist,
			}
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:674
		{
			yyVAL.ds = &DistributionDefinition{
				Replicated: true,
			}
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:682
		{
			yyVAL.strlist = yyDollar[3].strlist
		}
	case 91:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:684
		{
			/* empty column types should be prohibited */
			yyVAL.strlist = nil
		}
	case 92:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:690
		{
			yyVAL.strlist = append(yyDollar[1].strlist, yyDollar[3].str)
		}
	case 93:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:692
		{
			yyVAL.strlist = []string{
				yyDollar[1].str,
			}
		}
	case 94:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:699
		{
			yyVAL.str = "varchar"
		}
	case 95:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:701
		{
			yyVAL.str = "varchar hashed"
		}
	case 96:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:703
		{
			yyVAL.str = "integer"
		}
	case 97:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:705
		{
			yyVAL.str = "integer"
		}
	case 98:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:707
		{
			yyVAL.str = "uinteger"
		}
	case 99:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:709
		{
			yyVAL.str = "uinteger"
		}
	case 100:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:715
		{
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: yyDollar[3].str, TableName: yyDollar[4].str, Entries: yyDollar[5].entrieslist, Distribution: yyDollar[6].str}
		}
	case 101:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:720
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: "shrule" + str, TableName: yyDollar[3].str, Entries: yyDollar[4].entrieslist, Distribution: yyDollar[5].str}
		}
	case 102:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:729
		{
			yyVAL.entrieslist = make([]ShardingRuleEntry, 0)
			yyVAL.entrieslist = append(yyVAL.entrieslist, yyDollar[1].shruleEntry)
		}
	case 103:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:735
		{
			yyVAL.entrieslist = append(yyDollar[1].entrieslist, yyDollar[2].shruleEntry)
		}
	case 104:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:741
		{
			yyVAL.shruleEntry = ShardingRuleEntry{
				Column:       yyDollar[1].str,
				HashFunction: yyDollar[2].str,
			}
		}
	case 105:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:750
		{
			yyVAL.str = yyDollar[2].str
		}
	case 106:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:753
		{
			yyVAL.str = ""
		}
	case 107:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:757
		{
			yyVAL.str = yyDollar[2].str
		}
	case 108:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:762
		{
			yyVAL.str = yyDollar[2].str
		}
	case 109:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:768
		{
			yyVAL.str = "identity"
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:770
		{
			yyVAL.str = "murmur"
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:772
		{
			yyVAL.str = "city"
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:778
		{
			yyVAL.str = yyDollar[3].str
		}
	case 113:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:780
		{
			yyVAL.str = ""
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:785
		{
			yyVAL.str = yyDollar[3].str
		}
	case 115:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:787
		{
			yyVAL.str = "default"
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:792
		{
			yyVAL.bytes = []byte(yyDollar[1].str)
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:795
		{
			buf := make([]byte, 8)
			binary.PutVarint(buf, int64(yyDollar[1].uinteger))
			yyVAL.bytes = buf
		}
	case 118:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:802
		{
			yyVAL.krbound = &KeyRangeBound{
				Pivots: [][]byte{
					yyDollar[1].bytes,
				},
			}
		}
	case 119:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:809
		{
			yyVAL.krbound = &KeyRangeBound{
				Pivots: append(yyDollar[1].krbound.Pivots, yyDollar[3].bytes),
			}
		}
	case 120:
		yyDollar = yyS[yypt-9 : yypt+1]
//line gram.y:818
		{
			yyVAL.kr = &KeyRangeDefinition{
				KeyRangeID:   yyDollar[3].str,
				LowerBound:   yyDollar[5].krbound,
				ShardID:      yyDollar[8].str,
				Distribution: yyDollar[9].str,
			}
		}
	case 121:
		yyDollar = yyS[yypt-8 : yypt+1]
//line gram.y:827
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.kr = &KeyRangeDefinition{
				LowerBound:   yyDollar[4].krbound,
				ShardID:      yyDollar[7].str,
				Distribution: yyDollar[8].str,
				KeyRangeID:   "kr" + str,
			}
		}
	case 122:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:842
		{
			yyVAL.shard = &ShardDefinition{Id: yyDollar[2].str, Hosts: yyDollar[5].strlist}
		}
	case 123:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:847
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.shard = &ShardDefinition{Id: "shard" + str, Hosts: yyDollar[4].strlist}
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:857
		{
			yyVAL.strlist = []string{yyDollar[1].str}
		}
	case 125:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:862
		{
			yyVAL.strlist = append(yyDollar[1].strlist, yyDollar[3].str)
		}
	case 126:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:868
		{
			yyVAL.unlock = &Unlock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:874
		{
			yyVAL.sharding_rule_selector = &ShardingRuleSelector{ID: yyDollar[3].str}
		}
	case 128:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:880
		{
			yyVAL.key_range_selector = &KeyRangeSelector{KeyRangeID: yyDollar[3].str}
		}
	case 129:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:886
		{
			yyVAL.distribution_selector = &DistributionSelector{ID: yyDollar[2].str, Replicated: false}
		}
	case 130:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:888
		{
			yyVAL.distribution_selector = &DistributionSelector{Replicated: true, ID: "REPLICATED"}
		}
	case 131:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:894
		{
			yyVAL.split = &SplitKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeFromID: yyDollar[4].str, Border: yyDollar[6].krbound}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:900
		{
			yyVAL.kill = &Kill{Cmd: yyDollar[2].str, Target: yyDollar[3].uinteger}
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:903
		{
			yyVAL.kill = &Kill{Cmd: "client", Target: yyDollar[3].uinteger}
		}
	case 134:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:909
		{
			yyVAL.move = &MoveKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str}
		}
	case 135:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:915
		{
			yyVAL.redistribute = &RedistributeKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str, BatchSize: yyDollar[5].opt_batch_size, Check: true, Apply: true}
		}
	case 136:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:917
		{
			yyVAL.redistribute = &RedistributeKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str, BatchSize: yyDollar[5].opt_batch_size, Check: true}
		}
	case 137:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:919
		{
			yyVAL.redistribute = &RedistributeKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str, BatchSize: yyDollar[5].opt_batch_size, Apply: true}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:923
		{
			yyVAL.opt_batch_size = int(yyDollar[3].uinteger)
		}
	case 139:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:924
		{
			yyVAL.opt_batch_size = -1
		}
	case 140:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:928
		{
			yyVAL.unite = &UniteKeyRange{KeyRangeIDL: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeIDR: yyDollar[4].str}
		}
	case 141:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:934
		{
			yyVAL.listen = &Listen{addr: yyDollar[2].str}
		}
	case 142:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:940
		{
			yyVAL.shutdown = &Shutdown{}
		}
	case 143:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:946
		{
			yyVAL.invalidate_cache = &InvalidateCache{}
		}
	case 144:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:954
		{
			yyVAL.register_router = &RegisterRouter{ID: yyDollar[3].str, Addr: yyDollar[5].str}
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:960
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: yyDollar[3].str}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:965
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: `*`}
		}
	}
	goto yystack /* stack new state and value */
}
