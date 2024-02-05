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

	attach *AttachTable

	entrieslist []ShardingRuleEntry
	shruleEntry ShardingRuleEntry

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
const SHARDING = 57377
const COLUMN = 57378
const TABLE = 57379
const HASH = 57380
const FUNCTION = 57381
const KEY = 57382
const RANGE = 57383
const DISTRIBUTION = 57384
const SHARDS = 57385
const KEY_RANGES = 57386
const ROUTERS = 57387
const SHARD = 57388
const HOST = 57389
const SHARDING_RULES = 57390
const RULE = 57391
const COLUMNS = 57392
const VERSION = 57393
const BY = 57394
const FROM = 57395
const TO = 57396
const WITH = 57397
const UNITE = 57398
const ALL = 57399
const ADDRESS = 57400
const FOR = 57401
const CLIENT = 57402
const IDENTITY = 57403
const MURMUR = 57404
const CITY = 57405
const START = 57406
const STOP = 57407
const TRACE = 57408
const MESSAGES = 57409
const VARCHAR = 57410
const INTEGER = 57411
const INT = 57412
const TYPES = 57413
const OP = 57414

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
	"SHARDING",
	"COLUMN",
	"TABLE",
	"HASH",
	"FUNCTION",
	"KEY",
	"RANGE",
	"DISTRIBUTION",
	"SHARDS",
	"KEY_RANGES",
	"ROUTERS",
	"SHARD",
	"HOST",
	"SHARDING_RULES",
	"RULE",
	"COLUMNS",
	"VERSION",
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

//line gram.y:740

//line yacctab:1
var yyExca = [...]int8{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 230

var yyAct = [...]uint8{
	124, 162, 67, 157, 121, 95, 171, 133, 57, 130,
	132, 108, 88, 114, 27, 28, 158, 159, 160, 85,
	52, 51, 192, 193, 194, 135, 30, 29, 34, 35,
	66, 164, 21, 20, 24, 25, 26, 31, 32, 136,
	83, 78, 36, 84, 77, 128, 78, 81, 164, 78,
	112, 78, 78, 100, 202, 201, 198, 197, 172, 91,
	129, 99, 78, 138, 33, 98, 152, 87, 135, 79,
	142, 97, 22, 23, 113, 101, 102, 104, 44, 92,
	107, 110, 136, 45, 60, 43, 65, 117, 119, 46,
	115, 56, 181, 117, 118, 155, 54, 82, 58, 125,
	126, 127, 116, 120, 103, 90, 86, 80, 182, 200,
	137, 111, 199, 166, 139, 78, 143, 140, 109, 131,
	76, 106, 89, 196, 195, 188, 185, 148, 75, 74,
	38, 153, 53, 96, 179, 42, 167, 168, 154, 41,
	163, 169, 161, 94, 170, 174, 40, 173, 109, 175,
	39, 78, 177, 150, 63, 178, 91, 50, 59, 61,
	151, 49, 37, 180, 71, 72, 73, 1, 48, 163,
	18, 17, 47, 78, 145, 186, 183, 184, 187, 147,
	146, 69, 190, 189, 69, 122, 16, 176, 15, 14,
	68, 141, 12, 68, 70, 145, 13, 8, 203, 204,
	147, 146, 205, 206, 9, 207, 208, 209, 210, 105,
	156, 191, 165, 134, 19, 6, 5, 4, 3, 7,
	11, 10, 64, 62, 55, 2, 123, 149, 144, 93,
}

var yyPact = [...]int16{
	8, -1000, 115, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	43, 43, -45, -46, 56, 44, 44, 150, 26, 180,
	-1000, 44, 44, 44, 107, 106, 83, -1000, -1000, -1000,
	-1000, -1000, -1000, 147, 20, 66, 42, -1000, -1000, -1000,
	-1000, -17, -48, -1000, 65, -1000, 18, 89, 48, -1000,
	38, -1000, 135, -1000, 119, 119, -1000, -1000, -1000, -1000,
	-1000, 12, 7, -2, 147, 47, 147, 85, -1000, 111,
	58, -5, 27, -54, 119, -1000, 45, 37, -1000, -1000,
	89, -1000, 147, -1000, 169, -1000, -1000, -1000, 147, 147,
	147, -13, -1000, -1000, 6, -1000, -62, 81, 32, 147,
	10, 177, 23, 180, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 191, 169, 149, -1000, 14, -1000, -1000, 180, 53,
	-52, 32, -11, -1000, 75, 147, 147, -1000, 177, 4,
	4, -1000, 180, -1000, 169, -1000, -1000, -1000, 170, 180,
	-1000, -1000, 180, -1000, -1000, 147, 122, -1000, -1000, -1000,
	-1000, -11, -1000, -1000, 50, -1000, 69, -1000, -1000, 4,
	4, 103, 177, 102, -1000, 191, -1000, -1000, -1000, -52,
	-1000, 147, -39, 101, 100, 3, -1000, -1000, 2, -1000,
	-1000, -1000, -1000, 74, 71, 1, 0, 147, 147, -1000,
	-1000, 147, 147, -28, -28, -28, -28, -1000, -1000, -1000,
	-1000,
}

var yyPgo = [...]uint8{
	0, 229, 4, 228, 227, 226, 2, 0, 5, 225,
	224, 132, 8, 223, 222, 221, 220, 219, 218, 217,
	216, 215, 214, 150, 146, 139, 135, 10, 7, 11,
	213, 212, 211, 1, 210, 209, 3, 12, 204, 197,
	196, 192, 189, 188, 186, 171, 170, 167, 162, 6,
}

var yyR1 = [...]int8{
	0, 47, 48, 48, 9, 9, 9, 9, 9, 9,
	9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
	9, 8, 6, 6, 6, 7, 3, 3, 3, 4,
	4, 5, 2, 2, 2, 1, 1, 13, 14, 37,
	37, 17, 17, 17, 17, 17, 17, 18, 18, 18,
	18, 20, 20, 21, 22, 19, 19, 19, 19, 15,
	39, 23, 35, 35, 34, 34, 36, 36, 36, 24,
	24, 27, 27, 28, 29, 29, 30, 30, 32, 32,
	32, 31, 31, 33, 33, 49, 49, 49, 25, 25,
	25, 25, 26, 26, 38, 10, 11, 12, 42, 16,
	16, 43, 44, 41, 40, 45, 46, 46,
}

var yyR2 = [...]int8{
	0, 2, 0, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 3, 3, 3, 0, 2, 1, 1, 1,
	0, 2, 4, 2, 4, 3, 4, 2, 2, 2,
	2, 4, 4, 3, 5, 2, 2, 2, 2, 3,
	2, 3, 3, 0, 3, 1, 1, 1, 1, 6,
	5, 1, 2, 2, 2, 0, 2, 2, 1, 2,
	2, 3, 0, 3, 0, 2, 2, 0, 10, 10,
	9, 9, 5, 4, 2, 3, 3, 2, 6, 3,
	3, 4, 4, 2, 1, 5, 3, 3,
}

var yyChk = [...]int16{
	-1000, -47, -9, -18, -19, -20, -21, -17, -39, -38,
	-15, -16, -41, -40, -42, -43, -44, -45, -46, -22,
	25, 24, 64, 65, 26, 27, 28, 6, 7, 19,
	18, 29, 30, 56, 20, 21, 34, -48, 15, -23,
	-24, -25, -26, 42, 35, 40, 46, -23, -24, -25,
	-26, 66, 66, -11, 40, -10, 35, -12, 42, -11,
	40, -11, -13, 4, -14, 60, 4, -6, 13, 4,
	14, -11, -11, -11, 22, 22, 37, -7, 4, 49,
	41, -7, 55, 57, 60, 67, 41, 49, -37, 33,
	57, -7, 41, -1, 8, -8, 14, -8, 53, 54,
	55, -7, -7, 57, -7, -35, 36, -7, -29, 37,
	-7, 53, 55, 47, 67, -8, 57, -7, 57, -7,
	-37, -2, 16, -5, -7, -7, -7, -7, 58, 54,
	71, -29, -27, -28, -30, 36, 50, -7, 53, -6,
	-8, 14, 47, -6, -3, 4, 10, 9, -2, -4,
	4, 11, 52, -6, -12, 42, -34, -36, 68, 69,
	70, -27, -33, -28, 59, -31, 38, -7, -7, -6,
	-8, -49, 54, -49, -6, -2, 17, -6, -6, 12,
	-33, 42, 39, -49, -49, 23, -6, -8, 23, -36,
	-7, -32, 61, 62, 63, 23, 23, 54, 54, 38,
	38, 54, 54, -7, -7, -7, -7, -33, -33, -33,
	-33,
}

var yyDef = [...]int8{
	0, -2, 2, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	104, 0, 0, 0, 0, 0, 0, 1, 3, 47,
	48, 49, 50, 0, 0, 0, 0, 55, 56, 57,
	58, 0, 0, 41, 0, 43, 0, 40, 0, 60,
	0, 94, 35, 37, 0, 0, 38, 103, 22, 23,
	24, 0, 0, 0, 0, 0, 0, 63, 25, 75,
	0, 0, 0, 0, 0, 53, 0, 0, 45, 39,
	40, 97, 0, 59, 0, 99, 21, 100, 0, 0,
	0, 0, 106, 107, 0, 61, 0, 75, 0, 0,
	0, 0, 0, 0, 51, 52, 42, 96, 44, 95,
	46, 36, 0, 0, 31, 0, 101, 102, 0, 0,
	0, 0, 84, 71, 82, 0, 0, 74, 0, 87,
	87, 21, 0, 93, 0, 26, 27, 28, 0, 0,
	29, 30, 0, 105, 54, 0, 62, 65, 66, 67,
	68, 84, 70, 72, 0, 73, 0, 76, 77, 87,
	87, 0, 0, 0, 92, 34, 32, 33, 98, 0,
	69, 0, 0, 0, 0, 0, 85, 86, 0, 64,
	83, 81, 78, 0, 0, 0, 0, 0, 0, 79,
	80, 0, 0, 84, 84, 84, 84, 90, 91, 88,
	89,
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
	72,
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
//line gram.y:195
		{
		}
	case 3:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:196
		{
		}
	case 4:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:201
		{
			setParseTree(yylex, yyDollar[1].create)
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:205
		{
			setParseTree(yylex, yyDollar[1].create)
		}
	case 6:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:209
		{
			setParseTree(yylex, yyDollar[1].trace)
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:213
		{
			setParseTree(yylex, yyDollar[1].stoptrace)
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:217
		{
			setParseTree(yylex, yyDollar[1].drop)
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:221
		{
			setParseTree(yylex, yyDollar[1].lock)
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:225
		{
			setParseTree(yylex, yyDollar[1].unlock)
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:229
		{
			setParseTree(yylex, yyDollar[1].show)
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:233
		{
			setParseTree(yylex, yyDollar[1].kill)
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:237
		{
			setParseTree(yylex, yyDollar[1].listen)
		}
	case 14:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:241
		{
			setParseTree(yylex, yyDollar[1].shutdown)
		}
	case 15:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:245
		{
			setParseTree(yylex, yyDollar[1].split)
		}
	case 16:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:249
		{
			setParseTree(yylex, yyDollar[1].move)
		}
	case 17:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:253
		{
			setParseTree(yylex, yyDollar[1].unite)
		}
	case 18:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:257
		{
			setParseTree(yylex, yyDollar[1].register_router)
		}
	case 19:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:261
		{
			setParseTree(yylex, yyDollar[1].unregister_router)
		}
	case 20:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:265
		{
			setParseTree(yylex, yyDollar[1].attach)
		}
	case 21:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:270
		{
			yyVAL.uinteger = uint(yyDollar[1].uinteger)
		}
	case 22:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:275
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 23:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:279
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 24:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:281
		{
			yyVAL.str = strconv.Itoa(int(yyDollar[1].uinteger))
		}
	case 25:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:286
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 26:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:292
		{
			yyVAL.str = yyDollar[1].str
		}
	case 27:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:294
		{
			yyVAL.str = "AND"
		}
	case 28:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:296
		{
			yyVAL.str = "OR"
		}
	case 29:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:301
		{
			yyVAL.str = yyDollar[1].str
		}
	case 30:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:303
		{
			yyVAL.str = "="
		}
	case 31:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:309
		{
			yyVAL.colref = ColumnRef{
				ColName: yyDollar[1].str,
			}
		}
	case 32:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:317
		{
			yyVAL.where = yyDollar[2].where
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:320
		{
			yyVAL.where = WhereClauseLeaf{
				ColRef: yyDollar[1].colref,
				Op:     yyDollar[2].str,
				Value:  yyDollar[3].str,
			}
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:328
		{
			yyVAL.where = WhereClauseOp{
				Op:    yyDollar[2].str,
				Left:  yyDollar[1].where,
				Right: yyDollar[3].where,
			}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:338
		{
			yyVAL.where = WhereClauseEmpty{}
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:342
		{
			yyVAL.where = yyDollar[2].where
		}
	case 37:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:349
		{
			switch v := strings.ToLower(string(yyDollar[1].str)); v {
			case DatabasesStr, RoutersStr, PoolsStr, ShardsStr, BackendConnectionsStr, KeyRangesStr, ShardingRules, ClientsStr, StatusStr, DistributionsStr, VersionStr:
				yyVAL.str = v
			default:
				yyVAL.str = UnsupportedStr
			}
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:360
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
//line gram.y:370
		{
			yyVAL.bool = true
		}
	case 40:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:370
		{
			yyVAL.bool = false
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:374
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].key_range_selector}
		}
	case 42:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:378
		{
			yyVAL.drop = &Drop{Element: &KeyRangeSelector{KeyRangeID: `*`}}
		}
	case 43:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:382
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].sharding_rule_selector}
		}
	case 44:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:386
		{
			yyVAL.drop = &Drop{Element: &ShardingRuleSelector{ID: `*`}}
		}
	case 45:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:390
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].distribution_selector, CascadeDelete: yyDollar[3].bool}
		}
	case 46:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:394
		{
			yyVAL.drop = &Drop{Element: &DistributionSelector{ID: `*`}, CascadeDelete: yyDollar[4].bool}
		}
	case 47:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:401
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 48:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:406
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:411
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 50:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:415
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 51:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:421
		{
			yyVAL.trace = &TraceStmt{All: true}
		}
	case 52:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:424
		{
			yyVAL.trace = &TraceStmt{
				Client: yyDollar[4].uinteger,
			}
		}
	case 53:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:432
		{
			yyVAL.stoptrace = &StopTraceStmt{}
		}
	case 54:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:439
		{
			yyVAL.attach = &AttachTable{
				Table:        yyDollar[3].str,
				Distribution: yyDollar[5].distribution_selector,
			}
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:449
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 56:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:454
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 57:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:459
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 58:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:463
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:470
		{
			yyVAL.show = &Show{Cmd: yyDollar[2].str, Where: yyDollar[3].where}
		}
	case 60:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:476
		{
			yyVAL.lock = &Lock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:484
		{
			yyVAL.ds = &DistributionDefinition{
				ID:       yyDollar[2].str,
				ColTypes: yyDollar[3].strlist,
			}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:492
		{
			yyVAL.strlist = yyDollar[3].strlist
		}
	case 63:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:494
		{
			/* empty column types should be prohibited */
			yyVAL.strlist = nil
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:500
		{
			yyVAL.strlist = append(yyDollar[1].strlist, yyDollar[3].str)
		}
	case 65:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:502
		{
			yyVAL.strlist = []string{
				yyDollar[1].str,
			}
		}
	case 66:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:509
		{
			yyVAL.str = "varchar"
		}
	case 67:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:511
		{
			yyVAL.str = "integer"
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:513
		{
			yyVAL.str = "integer"
		}
	case 69:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:519
		{
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: yyDollar[3].str, TableName: yyDollar[4].str, Entries: yyDollar[5].entrieslist, Distribution: yyDollar[6].str}
		}
	case 70:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:524
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: "shrule" + str, TableName: yyDollar[3].str, Entries: yyDollar[4].entrieslist, Distribution: yyDollar[5].str}
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:533
		{
			yyVAL.entrieslist = make([]ShardingRuleEntry, 0)
			yyVAL.entrieslist = append(yyVAL.entrieslist, yyDollar[1].shruleEntry)
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:539
		{
			yyVAL.entrieslist = append(yyDollar[1].entrieslist, yyDollar[2].shruleEntry)
		}
	case 73:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:545
		{
			yyVAL.shruleEntry = ShardingRuleEntry{
				Column:       yyDollar[1].str,
				HashFunction: yyDollar[2].str,
			}
		}
	case 74:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:554
		{
			yyVAL.str = yyDollar[2].str
		}
	case 75:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:557
		{
			yyVAL.str = ""
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:561
		{
			yyVAL.str = yyDollar[2].str
		}
	case 77:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:566
		{
			yyVAL.str = yyDollar[2].str
		}
	case 78:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:572
		{
			yyVAL.str = "identity"
		}
	case 79:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:574
		{
			yyVAL.str = "murmur"
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:576
		{
			yyVAL.str = "city"
		}
	case 81:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:582
		{
			yyVAL.str = yyDollar[3].str
		}
	case 82:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:585
		{
			yyVAL.str = ""
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:588
		{
			yyVAL.str = yyDollar[3].str
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:591
		{
			yyVAL.str = "default"
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:594
		{
		}
	case 86:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:594
		{
		}
	case 87:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:594
		{
		}
	case 88:
		yyDollar = yyS[yypt-10 : yypt+1]
//line gram.y:598
		{
			yyVAL.kr = &KeyRangeDefinition{
				KeyRangeID:   yyDollar[3].str,
				LowerBound:   []byte(yyDollar[5].str),
				ShardID:      yyDollar[9].str,
				Distribution: yyDollar[10].str,
			}
		}
	case 89:
		yyDollar = yyS[yypt-10 : yypt+1]
//line gram.y:607
		{
			yyVAL.kr = &KeyRangeDefinition{
				KeyRangeID:   yyDollar[3].str,
				LowerBound:   []byte(strconv.FormatUint(uint64(yyDollar[5].uinteger), 10)),
				ShardID:      yyDollar[9].str,
				Distribution: yyDollar[10].str,
			}
		}
	case 90:
		yyDollar = yyS[yypt-9 : yypt+1]
//line gram.y:616
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
	case 91:
		yyDollar = yyS[yypt-9 : yypt+1]
//line gram.y:629
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
	case 92:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:645
		{
			yyVAL.shard = &ShardDefinition{Id: yyDollar[2].str, Hosts: []string{yyDollar[5].str}}
		}
	case 93:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:650
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.shard = &ShardDefinition{Id: "shard" + str, Hosts: []string{yyDollar[4].str}}
		}
	case 94:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:661
		{
			yyVAL.unlock = &Unlock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:667
		{
			yyVAL.sharding_rule_selector = &ShardingRuleSelector{ID: yyDollar[3].str}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:673
		{
			yyVAL.key_range_selector = &KeyRangeSelector{KeyRangeID: yyDollar[3].str}
		}
	case 97:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:679
		{
			yyVAL.distribution_selector = &DistributionSelector{ID: yyDollar[2].str}
		}
	case 98:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:685
		{
			yyVAL.split = &SplitKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeFromID: yyDollar[4].str, Border: []byte(yyDollar[6].str)}
		}
	case 99:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:691
		{
			yyVAL.kill = &Kill{Cmd: yyDollar[2].str, Target: yyDollar[3].uinteger}
		}
	case 100:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:694
		{
			yyVAL.kill = &Kill{Cmd: "client", Target: yyDollar[3].uinteger}
		}
	case 101:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:700
		{
			yyVAL.move = &MoveKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str}
		}
	case 102:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:706
		{
			yyVAL.unite = &UniteKeyRange{KeyRangeIDL: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeIDR: yyDollar[4].str}
		}
	case 103:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:712
		{
			yyVAL.listen = &Listen{addr: yyDollar[2].str}
		}
	case 104:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:718
		{
			yyVAL.shutdown = &Shutdown{}
		}
	case 105:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:726
		{
			yyVAL.register_router = &RegisterRouter{ID: yyDollar[3].str, Addr: yyDollar[5].str}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:732
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: yyDollar[3].str}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:737
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: `*`}
		}
	}
	goto yystack /* stack new state and value */
}
