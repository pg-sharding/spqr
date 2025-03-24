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
const SEQUENCE = 57393
const SHARDS = 57394
const KEY_RANGES = 57395
const ROUTERS = 57396
const SHARD = 57397
const HOST = 57398
const SHARDING_RULES = 57399
const RULE = 57400
const COLUMNS = 57401
const VERSION = 57402
const HOSTS = 57403
const SEQUENCES = 57404
const BY = 57405
const FROM = 57406
const TO = 57407
const WITH = 57408
const UNITE = 57409
const ALL = 57410
const ADDRESS = 57411
const FOR = 57412
const CLIENT = 57413
const BATCH = 57414
const SIZE = 57415
const INVALIDATE = 57416
const CACHE = 57417
const IDENTITY = 57418
const MURMUR = 57419
const CITY = 57420
const START = 57421
const STOP = 57422
const TRACE = 57423
const MESSAGES = 57424
const TASK = 57425
const GROUP = 57426
const VARCHAR = 57427
const INTEGER = 57428
const INT = 57429
const TYPES = 57430
const OP = 57431
const ASC = 57432
const DESC = 57433
const ORDER = 57434

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

//line gram.y:962

//line yacctab:1
var yyExca = [...]int8{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 290

var yyAct = [...]int16{
	148, 199, 194, 247, 202, 170, 169, 147, 168, 161,
	167, 173, 145, 160, 129, 156, 103, 244, 245, 158,
	176, 195, 196, 197, 144, 62, 108, 136, 100, 58,
	60, 57, 64, 89, 67, 238, 239, 240, 76, 65,
	219, 163, 188, 98, 201, 75, 99, 153, 90, 91,
	91, 133, 95, 119, 241, 230, 118, 91, 164, 91,
	117, 166, 91, 172, 116, 106, 107, 66, 211, 201,
	186, 177, 91, 134, 163, 102, 93, 157, 154, 113,
	115, 226, 88, 49, 67, 120, 121, 251, 50, 106,
	47, 164, 48, 234, 128, 131, 220, 51, 135, 109,
	92, 110, 139, 141, 101, 137, 94, 69, 227, 203,
	224, 139, 74, 140, 138, 29, 30, 149, 150, 151,
	152, 122, 142, 105, 96, 223, 253, 32, 31, 37,
	38, 165, 132, 23, 22, 26, 27, 28, 33, 34,
	222, 174, 91, 159, 39, 130, 35, 97, 127, 217,
	218, 125, 63, 124, 104, 190, 207, 207, 192, 182,
	189, 85, 46, 45, 204, 205, 42, 228, 208, 84,
	200, 191, 114, 198, 44, 43, 36, 206, 59, 174,
	259, 130, 91, 40, 209, 212, 55, 54, 24, 25,
	215, 213, 87, 56, 146, 207, 49, 216, 53, 52,
	225, 50, 210, 47, 221, 48, 68, 70, 200, 112,
	51, 78, 80, 81, 82, 83, 229, 231, 91, 232,
	77, 79, 72, 179, 235, 233, 184, 236, 181, 180,
	78, 242, 179, 185, 41, 248, 214, 181, 180, 77,
	171, 1, 249, 187, 250, 19, 18, 17, 16, 15,
	14, 255, 248, 254, 258, 256, 12, 13, 8, 9,
	260, 143, 243, 175, 126, 193, 155, 123, 21, 86,
	20, 237, 162, 257, 252, 246, 6, 5, 4, 3,
	7, 11, 10, 73, 71, 61, 2, 183, 178, 111,
}

var yyPact = [...]int16{
	109, -1000, 151, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 42, 155, -50, -52, -16, 61, 61, 218,
	41, 207, -1000, 61, 61, 61, 61, 147, 139, 34,
	-42, -1000, -1000, -1000, -1000, -1000, -1000, 214, 52, 18,
	59, 58, -1000, -1000, -1000, -1000, 104, -25, -54, -1000,
	57, -1000, 17, 121, 55, 214, -58, 51, -1000, 54,
	-1000, 201, -1000, 158, 158, -1000, -1000, -1000, -1000, -1000,
	0, -5, -9, -13, 214, 53, -1000, 117, 214, -1000,
	106, -1000, -1000, 138, 68, -15, 12, 214, -55, 158,
	-1000, 46, 45, -1000, -1000, 121, -1000, -1000, -1000, -1000,
	214, -60, 178, -1000, -1000, -1000, 214, 214, 214, 214,
	-22, -1000, -1000, -1000, 29, 28, -1000, -69, 102, 32,
	214, -3, 226, 2, 207, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -72, 8, 228, 178, 222, -1000, 7,
	-1000, -30, -1000, 207, 214, 28, -1000, 214, -64, 32,
	-1, -1000, 65, 214, 214, -1000, 226, 145, -1000, -1000,
	-1000, -1000, 207, 190, -1000, -1000, 5, 214, 178, -1000,
	-1000, -1000, 219, 207, -1000, -1000, 226, 110, -33, -1000,
	-1000, -1000, 48, 192, -1000, 96, 81, 66, -1, -1000,
	-1000, 33, -1000, 63, -1000, -1000, 144, 226, -10, 190,
	207, 214, -1000, 228, -1000, -1000, 183, -1000, -1000, 158,
	47, -64, -1000, -1000, -1000, -1000, 214, -41, -11, -1000,
	214, -1000, -73, -1000, 214, -1000, -1000, -1000, -1000, -1000,
	-1000, 214, -26, -1000, -1000, -1000, 75, -1000, 65, -26,
	-1000, 214, -1000, 214, -1000, -1000, -1000, 168, -1000, 214,
	-1000,
}

var yyPgo = [...]int16{
	0, 289, 12, 8, 10, 288, 287, 7, 6, 0,
	5, 286, 285, 178, 152, 284, 283, 282, 281, 280,
	279, 278, 277, 276, 175, 174, 163, 162, 13, 275,
	9, 274, 273, 3, 14, 272, 4, 271, 1, 270,
	269, 268, 267, 266, 15, 265, 264, 11, 2, 16,
	263, 262, 261, 259, 258, 257, 256, 250, 249, 248,
	247, 246, 245, 243, 241, 234,
}

var yyR1 = [...]int8{
	0, 64, 65, 65, 11, 11, 11, 11, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
	11, 11, 11, 10, 8, 8, 8, 9, 5, 5,
	5, 6, 6, 7, 2, 2, 2, 1, 1, 15,
	16, 49, 49, 19, 19, 19, 19, 19, 19, 19,
	19, 20, 20, 20, 20, 22, 22, 23, 39, 40,
	40, 29, 29, 33, 44, 44, 31, 31, 32, 32,
	43, 43, 42, 21, 21, 21, 21, 21, 51, 51,
	51, 50, 50, 52, 52, 17, 54, 24, 24, 46,
	46, 45, 45, 48, 48, 48, 48, 48, 48, 25,
	25, 28, 28, 30, 34, 34, 35, 35, 37, 37,
	37, 36, 36, 38, 38, 3, 3, 4, 4, 26,
	26, 27, 27, 47, 47, 53, 12, 13, 14, 14,
	57, 18, 18, 58, 59, 59, 59, 63, 63, 60,
	56, 55, 41, 61, 62, 62,
}

var yyR2 = [...]int8{
	0, 2, 0, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 3, 3, 3, 0, 2, 1,
	1, 1, 0, 2, 4, 2, 4, 3, 4, 3,
	3, 2, 2, 2, 2, 4, 4, 3, 2, 2,
	4, 3, 1, 2, 6, 2, 2, 0, 1, 3,
	1, 2, 2, 2, 2, 2, 2, 4, 1, 1,
	0, 4, 0, 3, 0, 5, 2, 3, 2, 3,
	0, 3, 1, 1, 2, 1, 1, 2, 2, 6,
	5, 1, 2, 2, 2, 0, 2, 2, 1, 1,
	1, 3, 0, 3, 0, 1, 1, 1, 3, 9,
	8, 5, 4, 1, 3, 2, 3, 3, 2, 2,
	6, 3, 3, 4, 5, 6, 6, 3, 0, 4,
	2, 1, 2, 5, 3, 3,
}

var yyChk = [...]int16{
	-1000, -64, -11, -20, -21, -22, -23, -19, -54, -53,
	-17, -18, -56, -55, -57, -58, -59, -60, -61, -62,
	-39, -41, 25, 24, 79, 80, 26, 27, 28, 6,
	7, 19, 18, 29, 30, 37, 67, 20, 21, 35,
	74, -65, 15, -24, -25, -26, -27, 48, 50, 41,
	46, 55, -24, -25, -26, -27, 38, 81, 81, -13,
	46, -12, 41, -14, 48, 55, 83, 50, -13, 46,
	-13, -15, 4, -16, 71, 4, -8, 13, 4, 14,
	-13, -13, -13, -13, 22, 22, -40, -14, 48, 75,
	-9, 4, 48, 58, 47, -9, 66, 43, 68, 71,
	82, 47, 58, -49, 33, 68, -9, -9, 84, 48,
	47, -1, 8, -10, 14, -10, 64, 65, 65, 66,
	-9, -9, 68, -42, 36, 34, -46, 42, -9, -34,
	43, -9, 64, 66, 61, -9, 82, -10, 68, -9,
	68, -9, -49, -52, 84, -2, 16, -7, -9, -9,
	-9, -9, -9, 69, 49, -43, -44, 49, 88, -34,
	-28, -30, -35, 42, 59, -9, 64, -4, -3, -8,
	-10, 14, 61, -47, -8, -50, 92, 63, -5, 4,
	10, 9, -2, -6, 4, 11, 63, -63, 72, -8,
	-9, -44, -9, -45, -48, 85, 86, 87, -28, -38,
	-30, 70, -36, 44, -9, -9, -4, 12, 23, -47,
	12, 63, -7, -2, 17, -8, -4, 39, 40, 73,
	48, 12, 44, 44, 44, -38, 48, 45, 23, -3,
	65, -8, -7, -10, 46, -48, -9, -37, 76, 77,
	78, 65, -9, -51, 90, 91, -29, -33, -9, -9,
	-38, 12, -31, 51, -36, -38, -33, -32, -9, 12,
	-9,
}

var yyDef = [...]int16{
	0, -2, 2, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
	21, 22, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 141, 0, 0, 0, 0, 0, 0, 0,
	0, 1, 3, 51, 52, 53, 54, 0, 0, 0,
	0, 0, 73, 74, 75, 76, 0, 0, 0, 43,
	0, 45, 0, 42, 0, 0, 0, 0, 86, 0,
	125, 37, 39, 0, 0, 40, 140, 24, 25, 26,
	0, 0, 0, 0, 0, 0, 58, 0, 0, 142,
	90, 27, 88, 105, 0, 0, 0, 0, 0, 0,
	57, 0, 0, 47, 41, 42, 128, 49, 50, 129,
	0, 84, 0, 131, 23, 132, 0, 0, 0, 0,
	0, 144, 145, 59, 0, 0, 87, 0, 105, 0,
	0, 0, 0, 0, 0, 77, 55, 56, 44, 127,
	46, 126, 48, 82, 0, 38, 0, 0, 33, 0,
	133, 138, 139, 0, 0, 72, 70, 0, 0, 0,
	114, 101, 112, 0, 0, 104, 0, 0, 117, 115,
	116, 23, 0, 122, 123, 85, 0, 0, 0, 28,
	29, 30, 0, 0, 31, 32, 0, 134, 0, 143,
	60, 71, 65, 89, 92, 93, 95, 96, 114, 100,
	102, 0, 103, 0, 106, 107, 0, 0, 0, 121,
	0, 0, 83, 36, 34, 35, 130, 135, 136, 0,
	0, 0, 94, 97, 98, 99, 0, 0, 0, 118,
	0, 124, 80, 137, 0, 91, 113, 111, 108, 109,
	110, 0, 114, 81, 78, 79, 67, 62, 112, 114,
	120, 0, 64, 0, 63, 119, 61, 66, 68, 0,
	69,
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
	92,
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
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:467
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 52:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:472
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 53:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:477
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:481
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 55:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:487
		{
			yyVAL.trace = &TraceStmt{All: true}
		}
	case 56:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:490
		{
			yyVAL.trace = &TraceStmt{
				Client: yyDollar[4].uinteger,
			}
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:498
		{
			yyVAL.stoptrace = &StopTraceStmt{}
		}
	case 58:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:504
		{
			yyVAL.alter = &Alter{Element: yyDollar[2].alter_distribution}
		}
	case 59:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:510
		{
			yyVAL.alter_distribution = &AlterDistribution{
				Element: &AttachRelation{
					Distribution: yyDollar[1].distribution_selector,
					Relations:    yyDollar[2].relations,
				},
			}
		}
	case 60:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:519
		{
			yyVAL.alter_distribution = &AlterDistribution{
				Element: &DetachRelation{
					Distribution: yyDollar[1].distribution_selector,
					RelationName: yyDollar[4].str,
				},
			}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:531
		{
			yyVAL.dEntrieslist = append(yyDollar[1].dEntrieslist, yyDollar[3].distrKeyEntry)
		}
	case 62:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:533
		{
			yyVAL.dEntrieslist = []DistributionKeyEntry{
				yyDollar[1].distrKeyEntry,
			}
		}
	case 63:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:543
		{
			yyVAL.distrKeyEntry = DistributionKeyEntry{
				Column:       yyDollar[1].str,
				HashFunction: yyDollar[2].str,
			}
		}
	case 64:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:552
		{
			yyVAL.distributed_relation = &DistributedRelation{
				Name:            yyDollar[2].str,
				DistributionKey: yyDollar[5].dEntrieslist,
				Sequences:       yyDollar[6].strlist,
			}
		}
	case 65:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:560
		{
			yyVAL.distributed_relation = &DistributedRelation{
				Name:               yyDollar[2].str,
				ReplicatedRelation: true,
			}
		}
	case 66:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:569
		{
			yyVAL.strlist = yyDollar[2].strlist
		}
	case 67:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:571
		{
			yyVAL.strlist = nil
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:576
		{
			yyVAL.strlist = []string{yyDollar[1].str}
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:578
		{
			yyVAL.strlist = append(yyDollar[1].strlist, yyDollar[3].str)
		}
	case 70:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:584
		{
			yyVAL.relations = []*DistributedRelation{yyDollar[1].distributed_relation}
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:586
		{
			yyVAL.relations = append(yyDollar[1].relations, yyDollar[2].distributed_relation)
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:591
		{
			yyVAL.relations = yyDollar[2].relations
		}
	case 73:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:597
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 74:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:602
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 75:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:607
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:612
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 77:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:617
		{
			yyVAL.create = &Create{
				Element: &ReferenceRelationDefinition{
					TableName: yyDollar[4].str,
				},
			}
		}
	case 78:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:626
		{
			yyVAL.opt_asc_desc = &SortByAsc{}
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:627
		{
			yyVAL.opt_asc_desc = &SortByDesc{}
		}
	case 80:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:628
		{
			yyVAL.opt_asc_desc = &SortByDefault{}
		}
	case 81:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:632
		{
			yyVAL.order_clause = &Order{Col: yyDollar[3].colref, OptAscDesc: yyDollar[4].opt_asc_desc}
		}
	case 82:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:635
		{
			yyVAL.order_clause = OrderClause(nil)
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:640
		{
			yyVAL.group_clause = GroupBy{Col: yyDollar[3].colref}
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:643
		{
			yyVAL.group_clause = GroupByClauseEmpty{}
		}
	case 85:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:648
		{
			yyVAL.show = &Show{Cmd: yyDollar[2].str, Where: yyDollar[3].where, GroupBy: yyDollar[4].group_clause, Order: yyDollar[5].order_clause}
		}
	case 86:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:653
		{
			yyVAL.lock = &Lock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 87:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:661
		{
			yyVAL.ds = &DistributionDefinition{
				ID:       yyDollar[2].str,
				ColTypes: yyDollar[3].strlist,
			}
		}
	case 88:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:668
		{
			yyVAL.ds = &DistributionDefinition{
				Replicated: true,
			}
		}
	case 89:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:676
		{
			yyVAL.strlist = yyDollar[3].strlist
		}
	case 90:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:678
		{
			/* empty column types should be prohibited */
			yyVAL.strlist = nil
		}
	case 91:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:684
		{
			yyVAL.strlist = append(yyDollar[1].strlist, yyDollar[3].str)
		}
	case 92:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:686
		{
			yyVAL.strlist = []string{
				yyDollar[1].str,
			}
		}
	case 93:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:693
		{
			yyVAL.str = "varchar"
		}
	case 94:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:695
		{
			yyVAL.str = "varchar hashed"
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:697
		{
			yyVAL.str = "integer"
		}
	case 96:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:699
		{
			yyVAL.str = "integer"
		}
	case 97:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:701
		{
			yyVAL.str = "uinteger"
		}
	case 98:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:703
		{
			yyVAL.str = "uinteger"
		}
	case 99:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:709
		{
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: yyDollar[3].str, TableName: yyDollar[4].str, Entries: yyDollar[5].entrieslist, Distribution: yyDollar[6].str}
		}
	case 100:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:714
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: "shrule" + str, TableName: yyDollar[3].str, Entries: yyDollar[4].entrieslist, Distribution: yyDollar[5].str}
		}
	case 101:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:723
		{
			yyVAL.entrieslist = make([]ShardingRuleEntry, 0)
			yyVAL.entrieslist = append(yyVAL.entrieslist, yyDollar[1].shruleEntry)
		}
	case 102:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:729
		{
			yyVAL.entrieslist = append(yyDollar[1].entrieslist, yyDollar[2].shruleEntry)
		}
	case 103:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:735
		{
			yyVAL.shruleEntry = ShardingRuleEntry{
				Column:       yyDollar[1].str,
				HashFunction: yyDollar[2].str,
			}
		}
	case 104:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:744
		{
			yyVAL.str = yyDollar[2].str
		}
	case 105:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:747
		{
			yyVAL.str = ""
		}
	case 106:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:751
		{
			yyVAL.str = yyDollar[2].str
		}
	case 107:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:756
		{
			yyVAL.str = yyDollar[2].str
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:762
		{
			yyVAL.str = "identity"
		}
	case 109:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:764
		{
			yyVAL.str = "murmur"
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:766
		{
			yyVAL.str = "city"
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:772
		{
			yyVAL.str = yyDollar[3].str
		}
	case 112:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:774
		{
			yyVAL.str = ""
		}
	case 113:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:779
		{
			yyVAL.str = yyDollar[3].str
		}
	case 114:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:781
		{
			yyVAL.str = "default"
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:786
		{
			yyVAL.bytes = []byte(yyDollar[1].str)
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:789
		{
			buf := make([]byte, 8)
			binary.PutVarint(buf, int64(yyDollar[1].uinteger))
			yyVAL.bytes = buf
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:796
		{
			yyVAL.krbound = &KeyRangeBound{
				Pivots: [][]byte{
					yyDollar[1].bytes,
				},
			}
		}
	case 118:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:803
		{
			yyVAL.krbound = &KeyRangeBound{
				Pivots: append(yyDollar[1].krbound.Pivots, yyDollar[3].bytes),
			}
		}
	case 119:
		yyDollar = yyS[yypt-9 : yypt+1]
//line gram.y:812
		{
			yyVAL.kr = &KeyRangeDefinition{
				KeyRangeID:   yyDollar[3].str,
				LowerBound:   yyDollar[5].krbound,
				ShardID:      yyDollar[8].str,
				Distribution: yyDollar[9].str,
			}
		}
	case 120:
		yyDollar = yyS[yypt-8 : yypt+1]
//line gram.y:821
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
	case 121:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:836
		{
			yyVAL.shard = &ShardDefinition{Id: yyDollar[2].str, Hosts: yyDollar[5].strlist}
		}
	case 122:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:841
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.shard = &ShardDefinition{Id: "shard" + str, Hosts: yyDollar[4].strlist}
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:851
		{
			yyVAL.strlist = []string{yyDollar[1].str}
		}
	case 124:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:856
		{
			yyVAL.strlist = append(yyDollar[1].strlist, yyDollar[3].str)
		}
	case 125:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:862
		{
			yyVAL.unlock = &Unlock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:868
		{
			yyVAL.sharding_rule_selector = &ShardingRuleSelector{ID: yyDollar[3].str}
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:874
		{
			yyVAL.key_range_selector = &KeyRangeSelector{KeyRangeID: yyDollar[3].str}
		}
	case 128:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:880
		{
			yyVAL.distribution_selector = &DistributionSelector{ID: yyDollar[2].str, Replicated: false}
		}
	case 129:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:882
		{
			yyVAL.distribution_selector = &DistributionSelector{Replicated: true}
		}
	case 130:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:888
		{
			yyVAL.split = &SplitKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeFromID: yyDollar[4].str, Border: yyDollar[6].krbound}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:894
		{
			yyVAL.kill = &Kill{Cmd: yyDollar[2].str, Target: yyDollar[3].uinteger}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:897
		{
			yyVAL.kill = &Kill{Cmd: "client", Target: yyDollar[3].uinteger}
		}
	case 133:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:903
		{
			yyVAL.move = &MoveKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str}
		}
	case 134:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:909
		{
			yyVAL.redistribute = &RedistributeKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str, BatchSize: yyDollar[5].opt_batch_size, Check: true, Apply: true}
		}
	case 135:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:911
		{
			yyVAL.redistribute = &RedistributeKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str, BatchSize: yyDollar[5].opt_batch_size, Check: true}
		}
	case 136:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:913
		{
			yyVAL.redistribute = &RedistributeKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str, BatchSize: yyDollar[5].opt_batch_size, Apply: true}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:917
		{
			yyVAL.opt_batch_size = int(yyDollar[3].uinteger)
		}
	case 138:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:918
		{
			yyVAL.opt_batch_size = -1
		}
	case 139:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:922
		{
			yyVAL.unite = &UniteKeyRange{KeyRangeIDL: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeIDR: yyDollar[4].str}
		}
	case 140:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:928
		{
			yyVAL.listen = &Listen{addr: yyDollar[2].str}
		}
	case 141:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:934
		{
			yyVAL.shutdown = &Shutdown{}
		}
	case 142:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:940
		{
			yyVAL.invalidate_cache = &InvalidateCache{}
		}
	case 143:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:948
		{
			yyVAL.register_router = &RegisterRouter{ID: yyDollar[3].str, Addr: yyDollar[5].str}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:954
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: yyDollar[3].str}
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:959
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: `*`}
		}
	}
	goto yystack /* stack new state and value */
}
