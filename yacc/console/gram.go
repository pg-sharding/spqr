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
const SHARDING = 57379
const COLUMN = 57380
const TABLE = 57381
const HASH = 57382
const FUNCTION = 57383
const KEY = 57384
const RANGE = 57385
const DISTRIBUTION = 57386
const RELATION = 57387
const SHARDS = 57388
const KEY_RANGES = 57389
const ROUTERS = 57390
const SHARD = 57391
const HOST = 57392
const SHARDING_RULES = 57393
const RULE = 57394
const COLUMNS = 57395
const VERSION = 57396
const HOSTS = 57397
const BY = 57398
const FROM = 57399
const TO = 57400
const WITH = 57401
const UNITE = 57402
const ALL = 57403
const ADDRESS = 57404
const FOR = 57405
const CLIENT = 57406
const IDENTITY = 57407
const MURMUR = 57408
const CITY = 57409
const START = 57410
const STOP = 57411
const TRACE = 57412
const MESSAGES = 57413
const TASK = 57414
const GROUP = 57415
const VARCHAR = 57416
const INTEGER = 57417
const INT = 57418
const TYPES = 57419
const OP = 57420

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
	"SHARDING",
	"COLUMN",
	"TABLE",
	"HASH",
	"FUNCTION",
	"KEY",
	"RANGE",
	"DISTRIBUTION",
	"RELATION",
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
	"BY",
	"FROM",
	"TO",
	"WITH",
	"UNITE",
	"ALL",
	"ADDRESS",
	"FOR",
	"CLIENT",
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
}

var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

//line gram.y:843

//line yacctab:1
var yyExca = [...]int8{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 243

var yyAct = [...]uint8{
	132, 177, 212, 172, 180, 152, 151, 144, 150, 129,
	156, 143, 116, 139, 92, 153, 173, 174, 175, 56,
	141, 97, 122, 89, 54, 52, 58, 51, 206, 207,
	208, 59, 179, 146, 87, 69, 68, 88, 136, 82,
	82, 82, 120, 82, 81, 106, 82, 85, 147, 209,
	200, 105, 155, 149, 60, 82, 104, 166, 179, 95,
	96, 121, 91, 83, 146, 140, 137, 196, 44, 193,
	202, 80, 98, 45, 181, 43, 90, 107, 108, 147,
	46, 95, 101, 103, 115, 118, 84, 62, 197, 117,
	114, 125, 127, 112, 86, 111, 67, 126, 124, 125,
	109, 27, 28, 94, 123, 133, 134, 135, 119, 128,
	82, 93, 57, 30, 29, 34, 35, 42, 148, 21,
	20, 24, 25, 26, 31, 32, 185, 157, 142, 77,
	36, 76, 185, 38, 41, 40, 102, 198, 168, 50,
	162, 170, 167, 186, 216, 117, 185, 182, 183, 79,
	39, 178, 169, 53, 176, 33, 49, 48, 184, 82,
	188, 157, 194, 22, 23, 100, 187, 164, 189, 191,
	82, 130, 47, 65, 165, 192, 159, 37, 195, 61,
	63, 161, 160, 1, 178, 73, 74, 75, 18, 190,
	71, 17, 199, 16, 201, 71, 15, 204, 203, 70,
	72, 210, 159, 213, 70, 154, 14, 161, 160, 12,
	214, 13, 215, 8, 9, 113, 218, 213, 217, 219,
	171, 138, 110, 78, 19, 205, 145, 211, 6, 5,
	4, 3, 7, 11, 10, 66, 64, 55, 2, 131,
	163, 158, 99,
}

var yyPact = [...]int16{
	95, -1000, 118, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	31, 31, -43, -45, -18, 45, 45, 169, 32, 186,
	-1000, 45, 45, 45, 109, 107, 27, -1000, -1000, -1000,
	-1000, -1000, -1000, 166, 11, 43, 35, -1000, -1000, -1000,
	-1000, -27, -48, -1000, 33, -1000, 10, 78, 42, 166,
	-52, -1000, 29, -1000, 157, -1000, 122, 122, -1000, -1000,
	-1000, -1000, -1000, -1, -7, -14, 166, 39, -1000, 59,
	166, 52, -1000, 106, 51, -17, 6, -49, 122, -1000,
	37, 36, -1000, -1000, 78, -1000, -1000, -1000, 166, -1000,
	155, -1000, -1000, -1000, 166, 166, 166, -24, -1000, -1000,
	-1000, 21, 20, -1000, -57, 50, 26, 166, -4, 191,
	-3, 186, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 198,
	155, 163, -1000, 1, -1000, -1000, 186, 166, 20, -1000,
	166, -58, 26, -5, -1000, 34, 166, 166, -1000, 191,
	120, -1000, -1000, -1000, -1000, 186, 148, -1000, 155, -1000,
	-1000, -1000, 172, 186, -1000, -1000, 191, -1000, -1000, -1000,
	25, 150, -1000, -1000, -1000, -1000, -5, -1000, -1000, 23,
	-1000, 47, -1000, -1000, 114, 191, -8, 148, 186, 198,
	-1000, -1000, 134, 28, -58, -1000, 166, -37, -9, -1000,
	166, -1000, 166, -1000, -1000, -1000, -1000, -1000, -1000, 166,
	-31, 132, -1000, 34, -31, -1000, 166, -1000, -1000, -1000,
}

var yyPgo = [...]uint8{
	0, 242, 9, 6, 8, 241, 240, 239, 5, 0,
	15, 238, 237, 153, 112, 236, 235, 234, 233, 232,
	231, 230, 229, 228, 150, 135, 134, 117, 11, 227,
	7, 2, 12, 226, 4, 225, 1, 224, 223, 222,
	221, 13, 220, 215, 10, 3, 14, 214, 213, 211,
	209, 206, 196, 193, 191, 188, 183, 177,
}

var yyR1 = [...]int8{
	0, 56, 57, 57, 11, 11, 11, 11, 11, 11,
	11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
	11, 10, 8, 8, 8, 9, 5, 5, 5, 6,
	6, 7, 2, 2, 2, 1, 1, 15, 16, 46,
	46, 19, 19, 19, 19, 19, 19, 19, 19, 20,
	20, 20, 20, 22, 22, 23, 37, 38, 38, 29,
	29, 31, 41, 40, 40, 39, 21, 21, 21, 21,
	17, 48, 24, 43, 43, 42, 42, 45, 45, 45,
	25, 25, 28, 28, 30, 32, 32, 33, 33, 35,
	35, 35, 34, 34, 36, 3, 3, 4, 4, 26,
	26, 27, 27, 44, 44, 47, 12, 13, 14, 51,
	18, 18, 52, 53, 50, 49, 54, 55, 55,
}

var yyR2 = [...]int8{
	0, 2, 0, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 3, 3, 3, 0, 2, 1, 1, 1,
	0, 2, 4, 2, 4, 3, 4, 3, 3, 2,
	2, 2, 2, 4, 4, 3, 2, 2, 4, 3,
	1, 2, 5, 1, 2, 2, 2, 2, 2, 2,
	3, 2, 3, 3, 0, 3, 1, 1, 1, 1,
	6, 5, 1, 2, 2, 2, 0, 2, 2, 1,
	1, 1, 3, 0, 3, 1, 1, 1, 3, 9,
	8, 5, 4, 1, 3, 2, 3, 3, 2, 6,
	3, 3, 4, 4, 2, 1, 5, 3, 3,
}

var yyChk = [...]int16{
	-1000, -56, -11, -20, -21, -22, -23, -19, -48, -47,
	-17, -18, -50, -49, -51, -52, -53, -54, -55, -37,
	25, 24, 68, 69, 26, 27, 28, 6, 7, 19,
	18, 29, 30, 60, 20, 21, 35, -57, 15, -24,
	-25, -26, -27, 44, 37, 42, 49, -24, -25, -26,
	-27, 70, 70, -13, 42, -12, 37, -14, 44, 49,
	72, -13, 42, -13, -15, 4, -16, 64, 4, -8,
	13, 4, 14, -13, -13, -13, 22, 22, -38, -14,
	44, -9, 4, 52, 43, -9, 59, 61, 64, 71,
	43, 52, -46, 33, 61, -9, -9, 73, 43, -1,
	8, -10, 14, -10, 57, 58, 59, -9, -9, 61,
	-39, 36, 34, -43, 38, -9, -32, 39, -9, 57,
	59, 55, 71, -10, 61, -9, 61, -9, -46, -2,
	16, -7, -9, -9, -9, -9, 62, 45, -40, -41,
	45, 77, -32, -28, -30, -33, 38, 53, -9, 57,
	-4, -3, -8, -10, 14, 55, -44, -8, -5, 4,
	10, 9, -2, -6, 4, 11, 56, -8, -9, -41,
	-9, -42, -45, 74, 75, 76, -28, -36, -30, 63,
	-34, 40, -9, -9, -4, 12, 23, -44, 12, -2,
	17, -8, -4, 44, 12, -36, 44, 41, 23, -3,
	58, -8, 42, -45, -9, -35, 65, 66, 67, 58,
	-9, -29, -31, -9, -9, -36, 12, -34, -36, -31,
}

var yyDef = [...]int8{
	0, -2, 2, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	115, 0, 0, 0, 0, 0, 0, 1, 3, 49,
	50, 51, 52, 0, 0, 0, 0, 66, 67, 68,
	69, 0, 0, 41, 0, 43, 0, 40, 0, 0,
	0, 71, 0, 105, 35, 37, 0, 0, 38, 114,
	22, 23, 24, 0, 0, 0, 0, 0, 56, 0,
	0, 74, 25, 86, 0, 0, 0, 0, 0, 55,
	0, 0, 45, 39, 40, 108, 47, 48, 0, 70,
	0, 110, 21, 111, 0, 0, 0, 0, 117, 118,
	57, 0, 0, 72, 0, 86, 0, 0, 0, 0,
	0, 0, 53, 54, 42, 107, 44, 106, 46, 36,
	0, 0, 31, 0, 112, 113, 0, 0, 65, 63,
	0, 0, 0, 0, 82, 93, 0, 0, 85, 0,
	0, 97, 95, 96, 21, 0, 102, 103, 0, 26,
	27, 28, 0, 0, 29, 30, 0, 116, 58, 64,
	0, 73, 76, 77, 78, 79, 0, 81, 83, 0,
	84, 0, 87, 88, 0, 0, 0, 101, 0, 34,
	32, 33, 109, 0, 0, 80, 0, 0, 0, 98,
	0, 104, 0, 75, 94, 92, 89, 90, 91, 0,
	0, 62, 60, 93, 0, 100, 0, 61, 99, 59,
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
	72, 73, 74, 75, 76, 77, 78,
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
//line gram.y:222
		{
		}
	case 3:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:223
		{
		}
	case 4:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:228
		{
			setParseTree(yylex, yyDollar[1].create)
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:232
		{
			setParseTree(yylex, yyDollar[1].create)
		}
	case 6:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:236
		{
			setParseTree(yylex, yyDollar[1].trace)
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:240
		{
			setParseTree(yylex, yyDollar[1].stoptrace)
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:244
		{
			setParseTree(yylex, yyDollar[1].drop)
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:248
		{
			setParseTree(yylex, yyDollar[1].lock)
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:252
		{
			setParseTree(yylex, yyDollar[1].unlock)
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:256
		{
			setParseTree(yylex, yyDollar[1].show)
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:260
		{
			setParseTree(yylex, yyDollar[1].kill)
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:264
		{
			setParseTree(yylex, yyDollar[1].listen)
		}
	case 14:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:268
		{
			setParseTree(yylex, yyDollar[1].shutdown)
		}
	case 15:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:272
		{
			setParseTree(yylex, yyDollar[1].split)
		}
	case 16:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:276
		{
			setParseTree(yylex, yyDollar[1].move)
		}
	case 17:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:280
		{
			setParseTree(yylex, yyDollar[1].unite)
		}
	case 18:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:284
		{
			setParseTree(yylex, yyDollar[1].register_router)
		}
	case 19:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:288
		{
			setParseTree(yylex, yyDollar[1].unregister_router)
		}
	case 20:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:292
		{
			setParseTree(yylex, yyDollar[1].alter)
		}
	case 21:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:297
		{
			yyVAL.uinteger = uint(yyDollar[1].uinteger)
		}
	case 22:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:302
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 23:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:306
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 24:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:308
		{
			yyVAL.str = strconv.Itoa(int(yyDollar[1].uinteger))
		}
	case 25:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:313
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 26:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:319
		{
			yyVAL.str = yyDollar[1].str
		}
	case 27:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:321
		{
			yyVAL.str = "AND"
		}
	case 28:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:323
		{
			yyVAL.str = "OR"
		}
	case 29:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:328
		{
			yyVAL.str = yyDollar[1].str
		}
	case 30:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:330
		{
			yyVAL.str = "="
		}
	case 31:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:336
		{
			yyVAL.colref = ColumnRef{
				ColName: yyDollar[1].str,
			}
		}
	case 32:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:344
		{
			yyVAL.where = yyDollar[2].where
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:347
		{
			yyVAL.where = WhereClauseLeaf{
				ColRef: yyDollar[1].colref,
				Op:     yyDollar[2].str,
				Value:  yyDollar[3].str,
			}
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:355
		{
			yyVAL.where = WhereClauseOp{
				Op:    yyDollar[2].str,
				Left:  yyDollar[1].where,
				Right: yyDollar[3].where,
			}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:365
		{
			yyVAL.where = WhereClauseEmpty{}
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:369
		{
			yyVAL.where = yyDollar[2].where
		}
	case 37:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:376
		{
			switch v := strings.ToLower(string(yyDollar[1].str)); v {
			case DatabasesStr, RoutersStr, PoolsStr, ShardsStr, BackendConnectionsStr, KeyRangesStr, ShardingRules, ClientsStr, StatusStr, DistributionsStr, VersionStr, RelationsStr, TaskGroupStr, PreparedStatementsStr:
				yyVAL.str = v
			default:
				yyVAL.str = UnsupportedStr
			}
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:387
		{
			switch v := string(yyDollar[1].str); v {
			case ClientStr:
				yyVAL.str = v
			default:
				yyVAL.str = "unsupp"
			}
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:397
		{
			yyVAL.bool = true
		}
	case 40:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:397
		{
			yyVAL.bool = false
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:401
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].key_range_selector}
		}
	case 42:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:405
		{
			yyVAL.drop = &Drop{Element: &KeyRangeSelector{KeyRangeID: `*`}}
		}
	case 43:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:409
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].sharding_rule_selector}
		}
	case 44:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:413
		{
			yyVAL.drop = &Drop{Element: &ShardingRuleSelector{ID: `*`}}
		}
	case 45:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:417
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].distribution_selector, CascadeDelete: yyDollar[3].bool}
		}
	case 46:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:421
		{
			yyVAL.drop = &Drop{Element: &DistributionSelector{ID: `*`}, CascadeDelete: yyDollar[4].bool}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:425
		{
			yyVAL.drop = &Drop{Element: &ShardSelector{ID: yyDollar[3].str}}
		}
	case 48:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:429
		{
			yyVAL.drop = &Drop{Element: &TaskGroupSelector{}}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:436
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 50:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:441
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 51:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:446
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 52:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:450
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 53:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:456
		{
			yyVAL.trace = &TraceStmt{All: true}
		}
	case 54:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:459
		{
			yyVAL.trace = &TraceStmt{
				Client: yyDollar[4].uinteger,
			}
		}
	case 55:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:467
		{
			yyVAL.stoptrace = &StopTraceStmt{}
		}
	case 56:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:473
		{
			yyVAL.alter = &Alter{Element: yyDollar[2].alter_distribution}
		}
	case 57:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:479
		{
			yyVAL.alter_distribution = &AlterDistribution{
				Element: &AttachRelation{
					Distribution: yyDollar[1].distribution_selector,
					Relations:    yyDollar[2].relations,
				},
			}
		}
	case 58:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:488
		{
			yyVAL.alter_distribution = &AlterDistribution{
				Element: &DetachRelation{
					Distribution: yyDollar[1].distribution_selector,
					RelationName: yyDollar[4].str,
				},
			}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:500
		{
			yyVAL.dEntrieslist = append(yyDollar[1].dEntrieslist, yyDollar[3].distrKeyEntry)
		}
	case 60:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:502
		{
			yyVAL.dEntrieslist = []DistributionKeyEntry{
				yyDollar[1].distrKeyEntry,
			}
		}
	case 61:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:512
		{
			yyVAL.distrKeyEntry = DistributionKeyEntry{
				Column:       yyDollar[1].str,
				HashFunction: yyDollar[2].str,
			}
		}
	case 62:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:521
		{
			yyVAL.distributed_relation = &DistributedRelation{
				Name:            yyDollar[2].str,
				DistributionKey: yyDollar[5].dEntrieslist,
			}
		}
	case 63:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:530
		{
			yyVAL.relations = []*DistributedRelation{yyDollar[1].distributed_relation}
		}
	case 64:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:532
		{
			yyVAL.relations = append(yyDollar[1].relations, yyDollar[2].distributed_relation)
		}
	case 65:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:537
		{
			yyVAL.relations = yyDollar[2].relations
		}
	case 66:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:543
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 67:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:548
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 68:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:553
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 69:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:557
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:564
		{
			yyVAL.show = &Show{Cmd: yyDollar[2].str, Where: yyDollar[3].where}
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:570
		{
			yyVAL.lock = &Lock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 72:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:578
		{
			yyVAL.ds = &DistributionDefinition{
				ID:       yyDollar[2].str,
				ColTypes: yyDollar[3].strlist,
			}
		}
	case 73:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:586
		{
			yyVAL.strlist = yyDollar[3].strlist
		}
	case 74:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:588
		{
			/* empty column types should be prohibited */
			yyVAL.strlist = nil
		}
	case 75:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:594
		{
			yyVAL.strlist = append(yyDollar[1].strlist, yyDollar[3].str)
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:596
		{
			yyVAL.strlist = []string{
				yyDollar[1].str,
			}
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:603
		{
			yyVAL.str = "varchar"
		}
	case 78:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:605
		{
			yyVAL.str = "integer"
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:607
		{
			yyVAL.str = "integer"
		}
	case 80:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:613
		{
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: yyDollar[3].str, TableName: yyDollar[4].str, Entries: yyDollar[5].entrieslist, Distribution: yyDollar[6].str}
		}
	case 81:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:618
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: "shrule" + str, TableName: yyDollar[3].str, Entries: yyDollar[4].entrieslist, Distribution: yyDollar[5].str}
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:627
		{
			yyVAL.entrieslist = make([]ShardingRuleEntry, 0)
			yyVAL.entrieslist = append(yyVAL.entrieslist, yyDollar[1].shruleEntry)
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:633
		{
			yyVAL.entrieslist = append(yyDollar[1].entrieslist, yyDollar[2].shruleEntry)
		}
	case 84:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:639
		{
			yyVAL.shruleEntry = ShardingRuleEntry{
				Column:       yyDollar[1].str,
				HashFunction: yyDollar[2].str,
			}
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:648
		{
			yyVAL.str = yyDollar[2].str
		}
	case 86:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:651
		{
			yyVAL.str = ""
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:655
		{
			yyVAL.str = yyDollar[2].str
		}
	case 88:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:660
		{
			yyVAL.str = yyDollar[2].str
		}
	case 89:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:666
		{
			yyVAL.str = "identity"
		}
	case 90:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:668
		{
			yyVAL.str = "murmur"
		}
	case 91:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:670
		{
			yyVAL.str = "city"
		}
	case 92:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:676
		{
			yyVAL.str = yyDollar[3].str
		}
	case 93:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:678
		{
			yyVAL.str = ""
		}
	case 94:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:683
		{
			yyVAL.str = yyDollar[3].str
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:688
		{
			yyVAL.bytes = []byte(yyDollar[1].str)
		}
	case 96:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:691
		{
			buf := make([]byte, 8)
			binary.PutVarint(buf, int64(yyDollar[1].uinteger))
			yyVAL.bytes = buf
		}
	case 97:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:698
		{
			yyVAL.krbound = &KeyRangeBound{
				Pivots: [][]byte{
					yyDollar[1].bytes,
				},
			}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:705
		{
			yyVAL.krbound = &KeyRangeBound{
				Pivots: append(yyDollar[1].krbound.Pivots, yyDollar[3].bytes),
			}
		}
	case 99:
		yyDollar = yyS[yypt-9 : yypt+1]
//line gram.y:714
		{
			yyVAL.kr = &KeyRangeDefinition{
				KeyRangeID:   yyDollar[3].str,
				LowerBound:   yyDollar[5].krbound,
				ShardID:      yyDollar[8].str,
				Distribution: yyDollar[9].str,
			}
		}
	case 100:
		yyDollar = yyS[yypt-8 : yypt+1]
//line gram.y:723
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
	case 101:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:738
		{
			yyVAL.shard = &ShardDefinition{Id: yyDollar[2].str, Hosts: yyDollar[5].strlist}
		}
	case 102:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:743
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.shard = &ShardDefinition{Id: "shard" + str, Hosts: yyDollar[4].strlist}
		}
	case 103:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:753
		{
			yyVAL.strlist = []string{yyDollar[1].str}
		}
	case 104:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:758
		{
			yyVAL.strlist = append(yyDollar[1].strlist, yyDollar[3].str)
		}
	case 105:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:764
		{
			yyVAL.unlock = &Unlock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:770
		{
			yyVAL.sharding_rule_selector = &ShardingRuleSelector{ID: yyDollar[3].str}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:776
		{
			yyVAL.key_range_selector = &KeyRangeSelector{KeyRangeID: yyDollar[3].str}
		}
	case 108:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:782
		{
			yyVAL.distribution_selector = &DistributionSelector{ID: yyDollar[2].str}
		}
	case 109:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:788
		{
			yyVAL.split = &SplitKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeFromID: yyDollar[4].str, Border: yyDollar[6].krbound}
		}
	case 110:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:794
		{
			yyVAL.kill = &Kill{Cmd: yyDollar[2].str, Target: yyDollar[3].uinteger}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:797
		{
			yyVAL.kill = &Kill{Cmd: "client", Target: yyDollar[3].uinteger}
		}
	case 112:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:803
		{
			yyVAL.move = &MoveKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str}
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:809
		{
			yyVAL.unite = &UniteKeyRange{KeyRangeIDL: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeIDR: yyDollar[4].str}
		}
	case 114:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:815
		{
			yyVAL.listen = &Listen{addr: yyDollar[2].str}
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:821
		{
			yyVAL.shutdown = &Shutdown{}
		}
	case 116:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:829
		{
			yyVAL.register_router = &RegisterRouter{ID: yyDollar[3].str, Addr: yyDollar[5].str}
		}
	case 117:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:835
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: yyDollar[3].str}
		}
	case 118:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:840
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: `*`}
		}
	}
	goto yystack /* stack new state and value */
}
