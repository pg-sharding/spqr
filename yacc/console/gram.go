// Code generated by goyacc -o gram.go -p yy gram.y. DO NOT EDIT.

//line gram.y:3
package spqrparser

import __yyfmt__ "fmt"

//line gram.y:3

import (
	"crypto/rand"
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

//line gram.y:24
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
const VARCHAR = 57414
const INTEGER = 57415
const INT = 57416
const TYPES = 57417
const OP = 57418

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

//line gram.y:824

//line yacctab:1
var yyExca = [...]int8{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 248

var yyAct = [...]uint8{
	128, 166, 151, 171, 213, 140, 97, 150, 174, 125,
	180, 139, 135, 112, 90, 167, 168, 169, 118, 137,
	87, 52, 51, 205, 206, 207, 142, 85, 66, 132,
	86, 173, 67, 116, 80, 80, 102, 80, 216, 215,
	211, 143, 27, 28, 79, 80, 80, 83, 210, 80,
	181, 173, 101, 145, 30, 29, 34, 35, 100, 93,
	21, 20, 24, 25, 26, 31, 32, 160, 149, 117,
	142, 36, 99, 89, 81, 103, 104, 113, 192, 93,
	136, 189, 111, 114, 133, 143, 201, 78, 65, 121,
	123, 122, 120, 119, 105, 121, 33, 94, 88, 115,
	84, 129, 130, 131, 22, 23, 92, 124, 44, 82,
	60, 193, 56, 45, 144, 43, 175, 54, 146, 58,
	46, 80, 147, 110, 108, 138, 107, 91, 209, 57,
	208, 199, 196, 75, 162, 161, 156, 164, 53, 42,
	74, 38, 80, 176, 177, 172, 41, 163, 178, 98,
	170, 40, 179, 39, 126, 219, 113, 183, 182, 184,
	187, 50, 185, 188, 59, 61, 77, 190, 49, 69,
	71, 72, 73, 48, 191, 47, 172, 153, 68, 70,
	96, 80, 155, 154, 197, 69, 63, 200, 198, 194,
	195, 153, 202, 203, 68, 148, 155, 154, 158, 37,
	1, 18, 214, 17, 186, 159, 16, 15, 14, 12,
	13, 217, 218, 8, 9, 109, 221, 222, 165, 134,
	214, 223, 224, 220, 225, 226, 227, 106, 76, 19,
	204, 141, 212, 6, 5, 4, 3, 7, 11, 10,
	64, 62, 55, 2, 127, 157, 152, 95,
}

var yyPact = [...]int16{
	36, -1000, 126, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	71, 71, -48, -49, 75, 68, 68, 182, 24, 165,
	-1000, 68, 68, 68, 118, 111, 43, -1000, -1000, -1000,
	-1000, -1000, -1000, 177, 22, 66, 41, -1000, -1000, -1000,
	-1000, -34, -51, -1000, 55, -1000, 21, 94, 45, -1000,
	54, -1000, 172, -1000, 135, 135, -1000, -1000, -1000, -1000,
	-1000, 1, -6, -23, 177, 33, -1000, 90, 177, 85,
	-1000, 117, 42, -26, 14, -53, 135, -1000, 31, 30,
	-1000, -1000, 94, -1000, 177, -1000, 138, -1000, -1000, -1000,
	177, 177, 177, -33, -1000, -1000, -1000, 39, 35, -1000,
	-56, 38, 32, 177, -4, 181, 13, 165, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 173, 138, 194, -1000, 11,
	-1000, -1000, 165, 177, 35, -1000, 177, -57, 32, -12,
	-1000, 76, 177, 177, -1000, 181, -8, -8, -1000, 165,
	147, -1000, 138, -1000, -1000, -1000, 187, 165, -1000, -1000,
	165, -1000, -1000, -1000, 37, 155, -1000, -1000, -1000, -1000,
	-12, -1000, -1000, 34, -1000, 70, -1000, -1000, -8, -8,
	109, 181, 108, 147, 165, 173, -1000, -1000, -1000, 44,
	-57, -1000, 177, -42, 107, 105, -10, -1000, -1000, -18,
	-1000, 177, -1000, -1000, -1000, -1000, -1000, -1000, -19, -20,
	177, 177, 143, -1000, 76, 177, 177, -32, -32, 177,
	-1000, -32, -32, -1000, -1000, -1000, -1000, -1000,
}

var yyPgo = [...]uint8{
	0, 247, 9, 246, 245, 244, 2, 0, 6, 243,
	242, 138, 129, 241, 240, 239, 238, 237, 236, 235,
	234, 233, 153, 151, 146, 139, 11, 232, 5, 4,
	13, 231, 8, 230, 3, 229, 228, 227, 219, 12,
	218, 215, 7, 1, 14, 214, 213, 210, 209, 208,
	207, 206, 203, 201, 200, 199, 10,
}

var yyR1 = [...]int8{
	0, 54, 55, 55, 9, 9, 9, 9, 9, 9,
	9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
	9, 8, 6, 6, 6, 7, 3, 3, 3, 4,
	4, 5, 2, 2, 2, 1, 1, 13, 14, 44,
	44, 17, 17, 17, 17, 17, 17, 18, 18, 18,
	18, 20, 20, 21, 35, 36, 36, 27, 27, 29,
	39, 38, 38, 37, 19, 19, 19, 19, 15, 46,
	22, 41, 41, 40, 40, 43, 43, 43, 23, 23,
	26, 26, 28, 30, 30, 31, 31, 33, 33, 33,
	32, 32, 34, 56, 56, 56, 24, 24, 24, 24,
	25, 25, 42, 42, 45, 10, 11, 12, 49, 16,
	16, 50, 51, 48, 47, 52, 53, 53,
}

var yyR2 = [...]int8{
	0, 2, 0, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 3, 3, 3, 0, 2, 1, 1, 1,
	0, 2, 4, 2, 4, 3, 4, 2, 2, 2,
	2, 4, 4, 3, 2, 2, 4, 3, 1, 2,
	5, 1, 2, 2, 2, 2, 2, 2, 3, 2,
	3, 3, 0, 3, 1, 1, 1, 1, 6, 5,
	1, 2, 2, 2, 0, 2, 2, 1, 1, 1,
	3, 0, 3, 2, 2, 0, 10, 10, 9, 9,
	5, 4, 1, 3, 2, 3, 3, 2, 6, 3,
	3, 4, 4, 2, 1, 5, 3, 3,
}

var yyChk = [...]int16{
	-1000, -54, -9, -18, -19, -20, -21, -17, -46, -45,
	-15, -16, -48, -47, -49, -50, -51, -52, -53, -35,
	25, 24, 68, 69, 26, 27, 28, 6, 7, 19,
	18, 29, 30, 60, 20, 21, 35, -55, 15, -22,
	-23, -24, -25, 44, 37, 42, 49, -22, -23, -24,
	-25, 70, 70, -11, 42, -10, 37, -12, 44, -11,
	42, -11, -13, 4, -14, 64, 4, -6, 13, 4,
	14, -11, -11, -11, 22, 22, -36, -12, 44, -7,
	4, 52, 43, -7, 59, 61, 64, 71, 43, 52,
	-44, 33, 61, -7, 43, -1, 8, -8, 14, -8,
	57, 58, 59, -7, -7, 61, -37, 36, 34, -41,
	38, -7, -30, 39, -7, 57, 59, 55, 71, -8,
	61, -7, 61, -7, -44, -2, 16, -5, -7, -7,
	-7, -7, 62, 45, -38, -39, 45, 75, -30, -26,
	-28, -31, 38, 53, -7, 57, -6, -8, 14, 55,
	-42, -6, -3, 4, 10, 9, -2, -4, 4, 11,
	56, -6, -7, -39, -7, -40, -43, 72, 73, 74,
	-26, -34, -28, 63, -32, 40, -7, -7, -6, -8,
	-56, 58, -56, -42, 12, -2, 17, -6, -6, 44,
	12, -34, 44, 41, -56, -56, 23, -6, -8, 23,
	-6, 42, -43, -7, -33, 65, 66, 67, 23, 23,
	58, 58, -27, -29, -7, 58, 58, -7, -7, 12,
	-32, -7, -7, -34, -34, -29, -34, -34,
}

var yyDef = [...]int8{
	0, -2, 2, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	114, 0, 0, 0, 0, 0, 0, 1, 3, 47,
	48, 49, 50, 0, 0, 0, 0, 64, 65, 66,
	67, 0, 0, 41, 0, 43, 0, 40, 0, 69,
	0, 104, 35, 37, 0, 0, 38, 113, 22, 23,
	24, 0, 0, 0, 0, 0, 54, 0, 0, 72,
	25, 84, 0, 0, 0, 0, 0, 53, 0, 0,
	45, 39, 40, 107, 0, 68, 0, 109, 21, 110,
	0, 0, 0, 0, 116, 117, 55, 0, 0, 70,
	0, 84, 0, 0, 0, 0, 0, 0, 51, 52,
	42, 106, 44, 105, 46, 36, 0, 0, 31, 0,
	111, 112, 0, 0, 63, 61, 0, 0, 0, 0,
	80, 91, 0, 0, 83, 0, 95, 95, 21, 0,
	101, 102, 0, 26, 27, 28, 0, 0, 29, 30,
	0, 115, 56, 62, 0, 71, 74, 75, 76, 77,
	0, 79, 81, 0, 82, 0, 85, 86, 95, 95,
	0, 0, 0, 100, 0, 34, 32, 33, 108, 0,
	0, 78, 0, 0, 0, 0, 0, 93, 94, 0,
	103, 0, 73, 92, 90, 87, 88, 89, 0, 0,
	0, 0, 60, 58, 91, 0, 0, 0, 0, 0,
	59, 0, 0, 98, 99, 57, 96, 97,
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
	72, 73, 74, 75, 76,
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
//line gram.y:211
		{
		}
	case 3:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:212
		{
		}
	case 4:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:217
		{
			setParseTree(yylex, yyDollar[1].create)
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:221
		{
			setParseTree(yylex, yyDollar[1].create)
		}
	case 6:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:225
		{
			setParseTree(yylex, yyDollar[1].trace)
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:229
		{
			setParseTree(yylex, yyDollar[1].stoptrace)
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:233
		{
			setParseTree(yylex, yyDollar[1].drop)
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:237
		{
			setParseTree(yylex, yyDollar[1].lock)
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:241
		{
			setParseTree(yylex, yyDollar[1].unlock)
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:245
		{
			setParseTree(yylex, yyDollar[1].show)
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:249
		{
			setParseTree(yylex, yyDollar[1].kill)
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:253
		{
			setParseTree(yylex, yyDollar[1].listen)
		}
	case 14:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:257
		{
			setParseTree(yylex, yyDollar[1].shutdown)
		}
	case 15:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:261
		{
			setParseTree(yylex, yyDollar[1].split)
		}
	case 16:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:265
		{
			setParseTree(yylex, yyDollar[1].move)
		}
	case 17:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:269
		{
			setParseTree(yylex, yyDollar[1].unite)
		}
	case 18:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:273
		{
			setParseTree(yylex, yyDollar[1].register_router)
		}
	case 19:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:277
		{
			setParseTree(yylex, yyDollar[1].unregister_router)
		}
	case 20:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:281
		{
			setParseTree(yylex, yyDollar[1].alter)
		}
	case 21:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:286
		{
			yyVAL.uinteger = uint(yyDollar[1].uinteger)
		}
	case 22:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:291
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 23:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:295
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 24:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:297
		{
			yyVAL.str = strconv.Itoa(int(yyDollar[1].uinteger))
		}
	case 25:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:302
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 26:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:308
		{
			yyVAL.str = yyDollar[1].str
		}
	case 27:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:310
		{
			yyVAL.str = "AND"
		}
	case 28:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:312
		{
			yyVAL.str = "OR"
		}
	case 29:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:317
		{
			yyVAL.str = yyDollar[1].str
		}
	case 30:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:319
		{
			yyVAL.str = "="
		}
	case 31:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:325
		{
			yyVAL.colref = ColumnRef{
				ColName: yyDollar[1].str,
			}
		}
	case 32:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:333
		{
			yyVAL.where = yyDollar[2].where
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:336
		{
			yyVAL.where = WhereClauseLeaf{
				ColRef: yyDollar[1].colref,
				Op:     yyDollar[2].str,
				Value:  yyDollar[3].str,
			}
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:344
		{
			yyVAL.where = WhereClauseOp{
				Op:    yyDollar[2].str,
				Left:  yyDollar[1].where,
				Right: yyDollar[3].where,
			}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:354
		{
			yyVAL.where = WhereClauseEmpty{}
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:358
		{
			yyVAL.where = yyDollar[2].where
		}
	case 37:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:365
		{
			switch v := strings.ToLower(string(yyDollar[1].str)); v {
			case DatabasesStr, RoutersStr, PoolsStr, ShardsStr, BackendConnectionsStr, KeyRangesStr, ShardingRules, ClientsStr, StatusStr, DistributionsStr, VersionStr, RelationsStr:
				yyVAL.str = v
			default:
				yyVAL.str = UnsupportedStr
			}
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:376
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
//line gram.y:386
		{
			yyVAL.bool = true
		}
	case 40:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:386
		{
			yyVAL.bool = false
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:390
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].key_range_selector}
		}
	case 42:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:394
		{
			yyVAL.drop = &Drop{Element: &KeyRangeSelector{KeyRangeID: `*`}}
		}
	case 43:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:398
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].sharding_rule_selector}
		}
	case 44:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:402
		{
			yyVAL.drop = &Drop{Element: &ShardingRuleSelector{ID: `*`}}
		}
	case 45:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:406
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].distribution_selector, CascadeDelete: yyDollar[3].bool}
		}
	case 46:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:410
		{
			yyVAL.drop = &Drop{Element: &DistributionSelector{ID: `*`}, CascadeDelete: yyDollar[4].bool}
		}
	case 47:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:417
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 48:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:422
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:427
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 50:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:431
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 51:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:437
		{
			yyVAL.trace = &TraceStmt{All: true}
		}
	case 52:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:440
		{
			yyVAL.trace = &TraceStmt{
				Client: yyDollar[4].uinteger,
			}
		}
	case 53:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:448
		{
			yyVAL.stoptrace = &StopTraceStmt{}
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:454
		{
			yyVAL.alter = &Alter{Element: yyDollar[2].alter_distribution}
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:460
		{
			yyVAL.alter_distribution = &AlterDistribution{
				Element: &AttachRelation{
					Distribution: yyDollar[1].distribution_selector,
					Relations:    yyDollar[2].relations,
				},
			}
		}
	case 56:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:469
		{
			yyVAL.alter_distribution = &AlterDistribution{
				Element: &DetachRelation{
					Distribution: yyDollar[1].distribution_selector,
					RelationName: yyDollar[4].str,
				},
			}
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:481
		{
			yyVAL.dEntrieslist = append(yyDollar[1].dEntrieslist, yyDollar[3].distrKeyEntry)
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:483
		{
			yyVAL.dEntrieslist = []DistributionKeyEntry{
				yyDollar[1].distrKeyEntry,
			}
		}
	case 59:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:493
		{
			yyVAL.distrKeyEntry = DistributionKeyEntry{
				Column:       yyDollar[1].str,
				HashFunction: yyDollar[2].str,
			}
		}
	case 60:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:502
		{
			yyVAL.distributed_relation = &DistributedRelation{
				Name:            yyDollar[2].str,
				DistributionKey: yyDollar[5].dEntrieslist,
			}
		}
	case 61:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:511
		{
			yyVAL.relations = []*DistributedRelation{yyDollar[1].distributed_relation}
		}
	case 62:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:513
		{
			yyVAL.relations = append(yyDollar[1].relations, yyDollar[2].distributed_relation)
		}
	case 63:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:518
		{
			yyVAL.relations = yyDollar[2].relations
		}
	case 64:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:524
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 65:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:529
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 66:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:534
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 67:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:538
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 68:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:545
		{
			yyVAL.show = &Show{Cmd: yyDollar[2].str, Where: yyDollar[3].where}
		}
	case 69:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:551
		{
			yyVAL.lock = &Lock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:559
		{
			yyVAL.ds = &DistributionDefinition{
				ID:       yyDollar[2].str,
				ColTypes: yyDollar[3].strlist,
			}
		}
	case 71:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:567
		{
			yyVAL.strlist = yyDollar[3].strlist
		}
	case 72:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:569
		{
			/* empty column types should be prohibited */
			yyVAL.strlist = nil
		}
	case 73:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:575
		{
			yyVAL.strlist = append(yyDollar[1].strlist, yyDollar[3].str)
		}
	case 74:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:577
		{
			yyVAL.strlist = []string{
				yyDollar[1].str,
			}
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:584
		{
			yyVAL.str = "varchar"
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:586
		{
			yyVAL.str = "integer"
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:588
		{
			yyVAL.str = "integer"
		}
	case 78:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:594
		{
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: yyDollar[3].str, TableName: yyDollar[4].str, Entries: yyDollar[5].entrieslist, Distribution: yyDollar[6].str}
		}
	case 79:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:599
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: "shrule" + str, TableName: yyDollar[3].str, Entries: yyDollar[4].entrieslist, Distribution: yyDollar[5].str}
		}
	case 80:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:608
		{
			yyVAL.entrieslist = make([]ShardingRuleEntry, 0)
			yyVAL.entrieslist = append(yyVAL.entrieslist, yyDollar[1].shruleEntry)
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:614
		{
			yyVAL.entrieslist = append(yyDollar[1].entrieslist, yyDollar[2].shruleEntry)
		}
	case 82:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:620
		{
			yyVAL.shruleEntry = ShardingRuleEntry{
				Column:       yyDollar[1].str,
				HashFunction: yyDollar[2].str,
			}
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:629
		{
			yyVAL.str = yyDollar[2].str
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:632
		{
			yyVAL.str = ""
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:636
		{
			yyVAL.str = yyDollar[2].str
		}
	case 86:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:641
		{
			yyVAL.str = yyDollar[2].str
		}
	case 87:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:647
		{
			yyVAL.str = "identity"
		}
	case 88:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:649
		{
			yyVAL.str = "murmur"
		}
	case 89:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:651
		{
			yyVAL.str = "city"
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:657
		{
			yyVAL.str = yyDollar[3].str
		}
	case 91:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:659
		{
			yyVAL.str = ""
		}
	case 92:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:664
		{
			yyVAL.str = yyDollar[3].str
		}
	case 93:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:669
		{
		}
	case 94:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:669
		{
		}
	case 95:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:669
		{
		}
	case 96:
		yyDollar = yyS[yypt-10 : yypt+1]
//line gram.y:673
		{
			yyVAL.kr = &KeyRangeDefinition{
				KeyRangeID:   yyDollar[3].str,
				LowerBound:   []byte(yyDollar[5].str),
				ShardID:      yyDollar[9].str,
				Distribution: yyDollar[10].str,
			}
		}
	case 97:
		yyDollar = yyS[yypt-10 : yypt+1]
//line gram.y:682
		{
			yyVAL.kr = &KeyRangeDefinition{
				KeyRangeID:   yyDollar[3].str,
				LowerBound:   []byte(strconv.FormatUint(uint64(yyDollar[5].uinteger), 10)),
				ShardID:      yyDollar[9].str,
				Distribution: yyDollar[10].str,
			}
		}
	case 98:
		yyDollar = yyS[yypt-9 : yypt+1]
//line gram.y:691
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.kr = &KeyRangeDefinition{
				LowerBound:   []byte(yyDollar[4].str),
				Distribution: yyDollar[8].str,
				ShardID:      yyDollar[9].str,
				KeyRangeID:   "kr" + str,
			}
		}
	case 99:
		yyDollar = yyS[yypt-9 : yypt+1]
//line gram.y:704
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.kr = &KeyRangeDefinition{
				LowerBound:   []byte(strconv.FormatUint(uint64(yyDollar[4].uinteger), 10)),
				ShardID:      yyDollar[8].str,
				KeyRangeID:   "kr" + str,
				Distribution: yyDollar[9].str,
			}
		}
	case 100:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:719
		{
			yyVAL.shard = &ShardDefinition{Id: yyDollar[2].str, Hosts: yyDollar[5].strlist}
		}
	case 101:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:724
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.shard = &ShardDefinition{Id: "shard" + str, Hosts: yyDollar[4].strlist}
		}
	case 102:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:734
		{
			yyVAL.strlist = []string{yyDollar[1].str}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:739
		{
			yyVAL.strlist = append(yyDollar[1].strlist, yyDollar[3].str)
		}
	case 104:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:745
		{
			yyVAL.unlock = &Unlock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:751
		{
			yyVAL.sharding_rule_selector = &ShardingRuleSelector{ID: yyDollar[3].str}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:757
		{
			yyVAL.key_range_selector = &KeyRangeSelector{KeyRangeID: yyDollar[3].str}
		}
	case 107:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:763
		{
			yyVAL.distribution_selector = &DistributionSelector{ID: yyDollar[2].str}
		}
	case 108:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:769
		{
			yyVAL.split = &SplitKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeFromID: yyDollar[4].str, Border: []byte(yyDollar[6].str)}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:775
		{
			yyVAL.kill = &Kill{Cmd: yyDollar[2].str, Target: yyDollar[3].uinteger}
		}
	case 110:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:778
		{
			yyVAL.kill = &Kill{Cmd: "client", Target: yyDollar[3].uinteger}
		}
	case 111:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:784
		{
			yyVAL.move = &MoveKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str}
		}
	case 112:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:790
		{
			yyVAL.unite = &UniteKeyRange{KeyRangeIDL: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeIDR: yyDollar[4].str}
		}
	case 113:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:796
		{
			yyVAL.listen = &Listen{addr: yyDollar[2].str}
		}
	case 114:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:802
		{
			yyVAL.shutdown = &Shutdown{}
		}
	case 115:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:810
		{
			yyVAL.register_router = &RegisterRouter{ID: yyDollar[3].str, Addr: yyDollar[5].str}
		}
	case 116:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:816
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: yyDollar[3].str}
		}
	case 117:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:821
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: `*`}
		}
	}
	goto yystack /* stack new state and value */
}
