// Code generated by goyacc -o gram.go -p yy gram.y. DO NOT EDIT.

//line gram.y:3
package spqrparser

import __yyfmt__ "fmt"

//line gram.y:3

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

//line gram.y:23
type yySymType struct {
	yys   int
	str   string
	byte  byte
	bytes []byte
	int   int
	bool  bool
	empty struct{}

	set       *Set
	statement Statement
	show      *Show

	drop   *Drop
	create *Create

	kill   *Kill
	lock   *Lock
	unlock *Unlock

	ds            *DataspaceDefinition
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

	entrieslist []ShardingRuleEntry
	shruleEntry ShardingRuleEntry

	sharding_rule_selector *ShardingRuleSelector
	key_range_selector     *KeyRangeSelector

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
const SCONST = 57354
const TSEMICOLON = 57355
const TOPENBR = 57356
const TCLOSEBR = 57357
const SHUTDOWN = 57358
const LISTEN = 57359
const REGISTER = 57360
const UNREGISTER = 57361
const ROUTER = 57362
const ROUTE = 57363
const CREATE = 57364
const ADD = 57365
const DROP = 57366
const LOCK = 57367
const UNLOCK = 57368
const SPLIT = 57369
const MOVE = 57370
const COMPOSE = 57371
const SET = 57372
const SHARDING = 57373
const COLUMN = 57374
const TABLE = 57375
const HASH = 57376
const FUNCTION = 57377
const KEY = 57378
const RANGE = 57379
const DATASPACE = 57380
const SHARDS = 57381
const KEY_RANGES = 57382
const ROUTERS = 57383
const SHARD = 57384
const HOST = 57385
const SHARDING_RULES = 57386
const RULE = 57387
const COLUMNS = 57388
const VERSION = 57389
const BY = 57390
const FROM = 57391
const TO = 57392
const WITH = 57393
const UNITE = 57394
const ALL = 57395
const ADDRESS = 57396
const FOR = 57397
const CLIENT = 57398
const START = 57399
const STOP = 57400
const TRACE = 57401
const MESSAGES = 57402
const OP = 57403

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
	"SCONST",
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
	"SHARDING",
	"COLUMN",
	"TABLE",
	"HASH",
	"FUNCTION",
	"KEY",
	"RANGE",
	"DATASPACE",
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
	"START",
	"STOP",
	"TRACE",
	"MESSAGES",
	"OP",
}
var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

//line gram.y:624

//line yacctab:1
var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 190

var yyAct = [...]int{

	115, 142, 68, 112, 100, 122, 106, 121, 84, 52,
	28, 29, 51, 82, 67, 119, 83, 144, 77, 124,
	31, 30, 35, 36, 77, 77, 21, 20, 25, 26,
	27, 32, 33, 125, 24, 77, 77, 104, 77, 95,
	167, 166, 144, 159, 76, 150, 94, 80, 127, 93,
	139, 124, 86, 78, 129, 105, 34, 101, 158, 88,
	76, 22, 23, 157, 43, 125, 66, 110, 91, 92,
	85, 146, 79, 108, 98, 96, 97, 61, 77, 99,
	102, 165, 81, 103, 107, 87, 109, 111, 39, 109,
	42, 164, 44, 57, 116, 117, 118, 45, 55, 43,
	59, 41, 126, 46, 120, 75, 128, 101, 130, 40,
	47, 74, 50, 53, 58, 38, 70, 135, 90, 77,
	77, 54, 140, 49, 69, 147, 148, 143, 141, 113,
	149, 48, 151, 64, 137, 152, 37, 1, 19, 154,
	132, 138, 155, 156, 18, 134, 133, 143, 60, 62,
	17, 153, 16, 160, 71, 72, 73, 15, 161, 162,
	132, 13, 163, 14, 9, 134, 133, 168, 169, 10,
	170, 171, 145, 123, 6, 5, 4, 3, 8, 12,
	11, 7, 65, 63, 56, 2, 114, 136, 131, 89,
}
var yyPact = [...]int{

	4, -1000, 102, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	61, 61, -47, -50, 26, 62, 41, 41, 129, 10,
	112, -1000, 41, 41, 41, 91, 85, -1000, -1000, -1000,
	-1000, -1000, -1000, 116, 8, 35, 31, -1000, -1000, -1000,
	-1000, -40, -52, -1000, -1000, 33, -1000, 7, -1000, 32,
	-1000, 22, -1000, 110, -1000, 112, 112, -1000, -1000, -1000,
	-1000, 0, -4, -12, 116, 21, -1000, -1000, 74, 34,
	-14, 12, -54, 116, -1000, 20, 14, -1000, 116, -1000,
	115, -1000, -1000, 116, 116, 116, -39, -1000, -1000, 24,
	19, 116, -1, 112, 11, 112, -1000, -1000, -1000, -1000,
	-1000, -1000, 156, 115, 130, -1000, 2, -1000, -1000, 112,
	19, -13, -1000, 37, 116, 116, -1000, 112, -5, 112,
	-1000, 115, -1000, -1000, -1000, 136, 112, -1000, -1000, 112,
	-1000, -13, -1000, -1000, 25, -1000, 23, -1000, -1000, -7,
	112, -1000, 156, -1000, -1000, -1000, -1000, 116, 116, 112,
	70, -1000, -1000, 60, -9, -10, 116, 116, -38, -38,
	-1000, -1000,
}
var yyPgo = [...]int{

	0, 189, 3, 188, 187, 186, 2, 0, 185, 184,
	121, 183, 182, 181, 180, 179, 178, 177, 176, 175,
	174, 88, 109, 101, 90, 7, 5, 4, 173, 172,
	1, 169, 164, 163, 161, 157, 152, 150, 144, 138,
	137, 136,
}
var yyR1 = [...]int{

	0, 40, 41, 41, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
	8, 6, 6, 7, 3, 3, 3, 4, 4, 5,
	2, 2, 2, 1, 1, 11, 12, 13, 16, 16,
	16, 16, 16, 16, 17, 17, 17, 17, 19, 19,
	20, 18, 18, 18, 18, 14, 32, 21, 22, 22,
	25, 25, 26, 27, 27, 28, 28, 29, 29, 30,
	30, 23, 23, 24, 24, 31, 9, 10, 35, 15,
	15, 36, 37, 34, 33, 38, 39, 39,
}
var yyR2 = [...]int{

	0, 2, 0, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	3, 3, 3, 0, 2, 1, 1, 2, 2, 4,
	2, 4, 2, 3, 2, 2, 2, 2, 4, 4,
	3, 2, 2, 2, 2, 3, 2, 2, 6, 5,
	1, 2, 2, 2, 0, 2, 2, 3, 0, 3,
	0, 11, 10, 5, 4, 2, 3, 3, 6, 3,
	3, 4, 4, 2, 1, 5, 3, 3,
}
var yyChk = [...]int{

	-1000, -40, -8, -17, -18, -19, -20, -13, -16, -32,
	-31, -14, -15, -34, -33, -35, -36, -37, -38, -39,
	23, 22, 57, 58, 30, 24, 25, 26, 6, 7,
	17, 16, 27, 28, 52, 18, 19, -41, 13, -21,
	-22, -23, -24, 38, 31, 36, 42, -21, -22, -23,
	-24, 59, 59, -21, -10, 36, -9, 31, -21, 38,
	-10, 36, -10, -11, 4, -12, 56, 4, -6, 12,
	4, -10, -10, -10, 20, 20, -7, 4, 45, 37,
	-7, 51, 53, 56, 60, 37, 45, 53, 37, -1,
	8, -6, -6, 49, 50, 51, -7, -7, 53, -7,
	-27, 33, -7, 49, 51, 43, 60, -7, 53, -7,
	53, -7, -2, 14, -5, -7, -7, -7, -7, 54,
	-27, -25, -26, -28, 32, 46, -7, 49, -6, 43,
	-6, -3, 4, 10, 9, -2, -4, 4, 11, 48,
	-6, -25, -30, -26, 55, -29, 34, -7, -7, -6,
	50, -6, -2, 15, -6, -6, -30, 38, 35, 50,
	-6, -7, -7, -6, 21, 21, 50, 50, -7, -7,
	-30, -30,
}
var yyDef = [...]int{

	0, -2, 2, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 84, 0, 0, 0, 0, 0, 1, 3, 44,
	45, 46, 47, 0, 0, 0, 0, 51, 52, 53,
	54, 0, 0, 37, 38, 0, 40, 0, 42, 0,
	56, 0, 75, 33, 35, 0, 0, 36, 83, 21,
	22, 0, 0, 0, 0, 0, 57, 23, 64, 0,
	0, 0, 0, 0, 50, 0, 0, 43, 0, 55,
	0, 79, 80, 0, 0, 0, 0, 86, 87, 64,
	0, 0, 0, 0, 0, 0, 48, 49, 39, 77,
	41, 76, 34, 0, 0, 29, 0, 81, 82, 0,
	0, 70, 60, 68, 0, 0, 63, 0, 0, 0,
	74, 0, 24, 25, 26, 0, 0, 27, 28, 0,
	85, 70, 59, 61, 0, 62, 0, 65, 66, 0,
	0, 73, 32, 30, 31, 78, 58, 0, 0, 0,
	0, 69, 67, 0, 0, 0, 0, 0, 70, 70,
	72, 71,
}
var yyTok1 = [...]int{

	1,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
}
var yyTok3 = [...]int{
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
	base := yyPact[state]
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && yyChk[yyAct[n]] == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || yyExca[i+1] != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := yyExca[i]
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
		token = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		token = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = yyTok3[i+0]
		if token == char {
			token = yyTok3[i+1]
			goto out
		}
	}

out:
	if token == 0 {
		token = yyTok2[1] /* unknown char */
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
	yyn = yyPact[yystate]
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
	yyn = yyAct[yyn]
	if yyChk[yyn] == yytoken { /* valid shift */
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
	yyn = yyDef[yystate]
	if yyn == -2 {
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && yyExca[xi+1] == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = yyExca[xi+0]
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = yyExca[xi+1]
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
				yyn = yyPact[yyS[yyp].yys] + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = yyAct[yyn] /* simulate a shift of "error" */
					if yyChk[yystate] == yyErrCode {
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

	yyp -= yyR2[yyn]
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = yyR1[yyn]
	yyg := yyPgo[yyn]
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = yyAct[yyg]
	} else {
		yystate = yyAct[yyj]
		if yyChk[yystate] != -yyn {
			yystate = yyAct[yyg]
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 2:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:168
		{
		}
	case 3:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:169
		{
		}
	case 4:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:174
		{
			setParseTree(yylex, yyDollar[1].create)
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:178
		{
			setParseTree(yylex, yyDollar[1].create)
		}
	case 6:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:182
		{
			setParseTree(yylex, yyDollar[1].trace)
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:186
		{
			setParseTree(yylex, yyDollar[1].stoptrace)
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:190
		{
			setParseTree(yylex, yyDollar[1].set)
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:194
		{
			setParseTree(yylex, yyDollar[1].drop)
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:198
		{
			setParseTree(yylex, yyDollar[1].lock)
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:202
		{
			setParseTree(yylex, yyDollar[1].unlock)
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:206
		{
			setParseTree(yylex, yyDollar[1].show)
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:210
		{
			setParseTree(yylex, yyDollar[1].kill)
		}
	case 14:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:214
		{
			setParseTree(yylex, yyDollar[1].listen)
		}
	case 15:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:218
		{
			setParseTree(yylex, yyDollar[1].shutdown)
		}
	case 16:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:222
		{
			setParseTree(yylex, yyDollar[1].split)
		}
	case 17:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:226
		{
			setParseTree(yylex, yyDollar[1].move)
		}
	case 18:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:230
		{
			setParseTree(yylex, yyDollar[1].unite)
		}
	case 19:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:234
		{
			setParseTree(yylex, yyDollar[1].register_router)
		}
	case 20:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:238
		{
			setParseTree(yylex, yyDollar[1].unregister_router)
		}
	case 21:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:243
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 22:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:247
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 23:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:252
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 24:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:258
		{
			yyVAL.str = yyDollar[1].str
		}
	case 25:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:260
		{
			yyVAL.str = "AND"
		}
	case 26:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:262
		{
			yyVAL.str = "OR"
		}
	case 27:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:267
		{
			yyVAL.str = yyDollar[1].str
		}
	case 28:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:269
		{
			yyVAL.str = "="
		}
	case 29:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:275
		{
			yyVAL.colref = ColumnRef{
				ColName: yyDollar[1].str,
			}
		}
	case 30:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:283
		{
			yyVAL.where = yyDollar[2].where
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:286
		{
			yyVAL.where = WhereClauseLeaf{
				ColRef: yyDollar[1].colref,
				Op:     yyDollar[2].str,
				Value:  yyDollar[3].str,
			}
		}
	case 32:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:294
		{
			yyVAL.where = WhereClauseOp{
				Op:    yyDollar[2].str,
				Left:  yyDollar[1].where,
				Right: yyDollar[3].where,
			}
		}
	case 33:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:304
		{
			yyVAL.where = WhereClauseEmpty{}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:308
		{
			yyVAL.where = yyDollar[2].where
		}
	case 35:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:315
		{
			switch v := strings.ToLower(string(yyDollar[1].str)); v {
			case DatabasesStr, RoutersStr, PoolsStr, ShardsStr, BackendConnectionsStr, KeyRangesStr, ShardingRules, ClientsStr, StatusStr, DataspacesStr, VersionStr:
				yyVAL.str = v
			default:
				yyVAL.str = UnsupportedStr
			}
		}
	case 36:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:326
		{
			switch v := string(yyDollar[1].str); v {
			case ClientStr:
				yyVAL.str = v
			default:
				yyVAL.str = "unsupp"
			}
		}
	case 37:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:337
		{
			yyVAL.set = &Set{Element: yyDollar[2].ds}
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:344
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].key_range_selector}
		}
	case 39:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:349
		{
			yyVAL.drop = &Drop{Element: &KeyRangeSelector{KeyRangeID: `*`}}
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:353
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].sharding_rule_selector}
		}
	case 41:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:358
		{
			yyVAL.drop = &Drop{Element: &ShardingRuleSelector{ID: `*`}}
		}
	case 42:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:362
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].ds}
		}
	case 43:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:367
		{
			yyVAL.drop = &Drop{Element: &DataspaceSelector{ID: `*`}}
		}
	case 44:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:373
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 45:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:378
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 46:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:383
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 47:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:387
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 48:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:394
		{
			yyVAL.trace = &TraceStmt{All: true}
		}
	case 49:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:397
		{
			yyVAL.trace = &TraceStmt{
				Client: yyDollar[4].str,
			}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:405
		{
			yyVAL.stoptrace = &StopTraceStmt{}
		}
	case 51:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:412
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 52:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:417
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 53:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:422
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:426
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 55:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:433
		{
			yyVAL.show = &Show{Cmd: yyDollar[2].str, Where: yyDollar[3].where}
		}
	case 56:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:439
		{
			yyVAL.lock = &Lock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 57:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:447
		{
			yyVAL.ds = &DataspaceDefinition{ID: yyDollar[2].str}
		}
	case 58:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:453
		{
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: yyDollar[3].str, TableName: yyDollar[4].str, Entries: yyDollar[5].entrieslist, Dataspace: yyDollar[6].str}
		}
	case 59:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:458
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: "shrule" + str, TableName: yyDollar[3].str, Entries: yyDollar[4].entrieslist, Dataspace: yyDollar[5].str}
		}
	case 60:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:467
		{
			yyVAL.entrieslist = make([]ShardingRuleEntry, 0)
			yyVAL.entrieslist = append(yyVAL.entrieslist, yyDollar[1].shruleEntry)
		}
	case 61:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:473
		{
			yyVAL.entrieslist = append(yyDollar[1].entrieslist, yyDollar[2].shruleEntry)
		}
	case 62:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:479
		{
			yyVAL.shruleEntry = ShardingRuleEntry{
				Column:       yyDollar[1].str,
				HashFunction: yyDollar[2].str,
			}
		}
	case 63:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:488
		{
			yyVAL.str = yyDollar[2].str
		}
	case 64:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:491
		{
			yyVAL.str = ""
		}
	case 65:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:495
		{
			yyVAL.str = yyDollar[2].str
		}
	case 66:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:500
		{
			yyVAL.str = yyDollar[2].str
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:506
		{
			yyVAL.str = yyDollar[3].str
		}
	case 68:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:509
		{
			yyVAL.str = ""
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:512
		{
			yyVAL.str = yyDollar[3].str
		}
	case 70:
		yyDollar = yyS[yypt-0 : yypt+1]
//line gram.y:515
		{
			yyVAL.str = "default"
		}
	case 71:
		yyDollar = yyS[yypt-11 : yypt+1]
//line gram.y:520
		{
			yyVAL.kr = &KeyRangeDefinition{LowerBound: []byte(yyDollar[5].str), UpperBound: []byte(yyDollar[7].str), ShardID: yyDollar[10].str, KeyRangeID: yyDollar[3].str, Dataspace: yyDollar[11].str}
		}
	case 72:
		yyDollar = yyS[yypt-10 : yypt+1]
//line gram.y:524
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.kr = &KeyRangeDefinition{LowerBound: []byte(yyDollar[4].str), UpperBound: []byte(yyDollar[6].str), ShardID: yyDollar[9].str, KeyRangeID: "kr" + str, Dataspace: yyDollar[10].str}
		}
	case 73:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:535
		{
			yyVAL.shard = &ShardDefinition{Id: yyDollar[2].str, Hosts: []string{yyDollar[5].str}}
		}
	case 74:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:540
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.shard = &ShardDefinition{Id: "shard" + str, Hosts: []string{yyDollar[4].str}}
		}
	case 75:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:551
		{
			yyVAL.unlock = &Unlock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 76:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:557
		{
			yyVAL.sharding_rule_selector = &ShardingRuleSelector{ID: yyDollar[3].str}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:563
		{
			yyVAL.key_range_selector = &KeyRangeSelector{KeyRangeID: yyDollar[3].str}
		}
	case 78:
		yyDollar = yyS[yypt-6 : yypt+1]
//line gram.y:569
		{
			yyVAL.split = &SplitKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeFromID: yyDollar[4].str, Border: []byte(yyDollar[6].str)}
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:575
		{
			yyVAL.kill = &Kill{Cmd: yyDollar[2].str, Target: yyDollar[3].str}
		}
	case 80:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:578
		{
			yyVAL.kill = &Kill{Cmd: "client", Target: yyDollar[3].str}
		}
	case 81:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:584
		{
			yyVAL.move = &MoveKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str}
		}
	case 82:
		yyDollar = yyS[yypt-4 : yypt+1]
//line gram.y:590
		{
			yyVAL.unite = &UniteKeyRange{KeyRangeIDL: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeIDR: yyDollar[4].str}
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
//line gram.y:596
		{
			yyVAL.listen = &Listen{addr: yyDollar[2].str}
		}
	case 84:
		yyDollar = yyS[yypt-1 : yypt+1]
//line gram.y:602
		{
			yyVAL.shutdown = &Shutdown{}
		}
	case 85:
		yyDollar = yyS[yypt-5 : yypt+1]
//line gram.y:610
		{
			yyVAL.register_router = &RegisterRouter{ID: yyDollar[3].str, Addr: yyDollar[5].str}
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:616
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: yyDollar[3].str}
		}
	case 87:
		yyDollar = yyS[yypt-3 : yypt+1]
//line gram.y:621
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: `*`}
		}
	}
	goto yystack /* stack new state and value */
}
