// Code generated by goyacc -o yacc/console/sql.go -p yy yacc/console/sql.y. DO NOT EDIT.

//line yacc/console/sql.y:3

package spqrparser

import __yyfmt__ "fmt"

//line yacc/console/sql.y:4

//line yacc/console/sql.y:11
type yySymType struct {
	yys               int
	empty             struct{}
	statement         Statement
	show              *Show
	kr                *AddKeyRange
	shard             *AddShard
	register_router   *RegisterRouter
	unregister_router *UnregisterRouter
	kill              *Kill
	drop              *Drop
	add               *Add
	dropAll           *DropAll
	lock              *Lock
	shutdown          *Shutdown
	listen            *Listen
	unlock            *Unlock
	split             *SplitKeyRange
	move              *MoveKeyRange
	unite             *UniteKeyRange
	str               string
	byte              byte
	bytes             []byte
	int               int
	bool              bool
}

const STRING = 57346
const COMMAND = 57347
const SHOW = 57348
const KILL = 57349
const POOLS = 57350
const STATS = 57351
const LISTS = 57352
const SERVERS = 57353
const CLIENTS = 57354
const DATABASES = 57355
const SHUTDOWN = 57356
const LISTEN = 57357
const REGISTER = 57358
const UNREGISTER = 57359
const ROUTER = 57360
const ROUTE = 57361
const CREATE = 57362
const ADD = 57363
const DROP = 57364
const LOCK = 57365
const UNLOCK = 57366
const SPLIT = 57367
const MOVE = 57368
const SHARDING = 57369
const COLUMN = 57370
const KEY = 57371
const RANGE = 57372
const SHARDS = 57373
const KEY_RANGES = 57374
const ROUTERS = 57375
const SHARD = 57376
const HOST = 57377
const SHARDING_RULES = 57378
const RULE = 57379
const COLUMNS = 57380
const BY = 57381
const FROM = 57382
const TO = 57383
const WITH = 57384
const UNITE = 57385
const ALL = 57386
const ADDRESS = 57387

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"STRING",
	"COMMAND",
	"SHOW",
	"KILL",
	"POOLS",
	"STATS",
	"LISTS",
	"SERVERS",
	"CLIENTS",
	"DATABASES",
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
	"SHARDING",
	"COLUMN",
	"KEY",
	"RANGE",
	"SHARDS",
	"KEY_RANGES",
	"ROUTERS",
	"SHARD",
	"HOST",
	"SHARDING_RULES",
	"RULE",
	"COLUMNS",
	"BY",
	"FROM",
	"TO",
	"WITH",
	"UNITE",
	"ALL",
	"ADDRESS",
	"';'",
}
var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

//line yacc/console/sql.y:399

//line yacctab:1
var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 121

var yyAct = [...]int{

	68, 105, 81, 73, 58, 27, 28, 38, 94, 82,
	88, 74, 102, 30, 29, 34, 24, 85, 118, 113,
	22, 23, 35, 36, 31, 32, 101, 100, 97, 47,
	52, 114, 50, 49, 48, 98, 46, 70, 67, 40,
	80, 39, 33, 99, 79, 77, 41, 76, 75, 90,
	87, 72, 51, 53, 54, 71, 66, 55, 42, 65,
	43, 64, 62, 61, 60, 57, 117, 78, 63, 44,
	69, 106, 82, 59, 89, 108, 104, 84, 91, 92,
	93, 74, 95, 96, 37, 1, 107, 83, 103, 86,
	21, 20, 19, 18, 17, 15, 16, 25, 11, 26,
	12, 5, 111, 110, 109, 112, 4, 3, 8, 7,
	6, 9, 10, 14, 13, 115, 116, 56, 45, 119,
	2,
}
var yyPact = [...]int{

	-1, -1000, -39, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 12, 31, 51, -1000, -1000, 21, 21, 69,
	-1000, 35, 34, 33, 50, 32, 30, -1000, -1000, 26,
	1, 66, 0, 25, 7, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	18, 17, 15, 77, 14, 10, 68, 73, -25, -1000,
	6, 5, -1000, -1000, -1000, 68, 68, 68, -37, 68,
	68, -12, -1000, -3, -1000, 8, -1000, -1000, -1000, -1000,
	-1000, -13, -15, -30, 72, -1000, -1000, 67, 71, 69,
	68, 66, 68, -1000, -1000, -22, -1000, -1000, -1000, -1000,
	-8, -1000, -1000, 67, 67, 47, -1000, -23, 66, -1000,
}
var yyPgo = [...]int{

	0, 120, 118, 117, 114, 113, 112, 111, 110, 109,
	108, 107, 106, 101, 100, 99, 98, 97, 96, 95,
	94, 93, 92, 91, 90, 36, 89, 0, 4, 1,
	2, 3, 88, 87, 86, 85, 84,
}
var yyR1 = [...]int{

	0, 35, 36, 36, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 25, 25, 25, 25, 25, 25, 25,
	25, 25, 2, 3, 4, 34, 33, 26, 29, 30,
	27, 28, 16, 12, 13, 11, 14, 6, 7, 17,
	15, 20, 5, 21, 22, 19, 18, 32, 31, 23,
	8, 9, 10, 24,
}
var yyR2 = [...]int{

	0, 2, 0, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 2, 1, 1, 1, 1, 1,
	1, 1, 1, 11, 6, 6, 1, 4, 4, 4,
	4, 8, 2, 6, 6, 2, 1, 1, 1, 5,
	4, 4, 3, 3,
}
var yyChk = [...]int{

	-1000, -35, -1, -11, -12, -13, -8, -9, -10, -7,
	-6, -16, -14, -4, -5, -19, -18, -20, -21, -22,
	-23, -24, 21, 22, 17, -17, -15, 6, 7, 15,
	14, 25, 26, 43, 16, 23, 24, -36, 46, 29,
	27, 34, 27, 29, 18, -2, -25, 8, 13, 12,
	11, 31, 9, 32, 33, 36, -3, -25, -28, 4,
	29, 29, 29, 18, 29, 29, 30, 37, -27, 4,
	37, 30, 44, -31, 4, 30, 30, 30, -31, 30,
	30, -30, 4, -33, 4, 42, -26, 44, 4, -30,
	44, -30, -30, -30, 45, -30, -30, 40, 38, 35,
	40, 41, 42, -32, 4, -29, 4, -34, 4, -28,
	-30, -27, -30, 41, 39, -29, -29, 19, 41, -27,
}
var yyDef = [...]int{

	0, -2, 2, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
	21, 22, 0, 0, 0, 42, 46, 0, 0, 0,
	56, 0, 0, 0, 0, 0, 0, 1, 3, 0,
	0, 0, 0, 0, 0, 34, 32, 23, 24, 25,
	26, 27, 28, 29, 30, 31, 52, 33, 55, 41,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 40,
	0, 0, 62, 63, 58, 0, 0, 0, 0, 0,
	0, 0, 39, 0, 36, 0, 47, 61, 37, 48,
	60, 0, 0, 0, 0, 49, 50, 0, 0, 0,
	0, 0, 0, 59, 57, 0, 38, 44, 35, 45,
	0, 53, 54, 0, 0, 0, 51, 0, 0, 43,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 46,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45,
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
//line yacc/console/sql.y:102
		{
		}
	case 3:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:103
		{
		}
	case 4:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:108
		{
			setParseTree(yylex, yyDollar[1].add)
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:112
		{
			setParseTree(yylex, yyDollar[1].add)
		}
	case 6:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:116
		{
			setParseTree(yylex, yyDollar[1].add)
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:120
		{
			setParseTree(yylex, yyDollar[1].dropAll)
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:124
		{
			setParseTree(yylex, yyDollar[1].dropAll)
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:128
		{
			setParseTree(yylex, yyDollar[1].dropAll)
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:132
		{
			setParseTree(yylex, yyDollar[1].drop)
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:136
		{
			setParseTree(yylex, yyDollar[1].drop)
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:140
		{
			setParseTree(yylex, yyDollar[1].lock)
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:144
		{
			setParseTree(yylex, yyDollar[1].unlock)
		}
	case 14:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:148
		{
			setParseTree(yylex, yyDollar[1].show)
		}
	case 15:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:152
		{
			setParseTree(yylex, yyDollar[1].kill)
		}
	case 16:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:156
		{
			setParseTree(yylex, yyDollar[1].listen)
		}
	case 17:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:160
		{
			setParseTree(yylex, yyDollar[1].shutdown)
		}
	case 18:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:164
		{
			setParseTree(yylex, yyDollar[1].split)
		}
	case 19:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:168
		{
			setParseTree(yylex, yyDollar[1].move)
		}
	case 20:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:172
		{
			setParseTree(yylex, yyDollar[1].unite)
		}
	case 21:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:176
		{
			setParseTree(yylex, yyDollar[1].register_router)
		}
	case 22:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:180
		{
			setParseTree(yylex, yyDollar[1].unregister_router)
		}
	case 32:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:197
		{
			switch v := string(yyDollar[1].str); v {
			case ShowDatabasesStr, ShowRoutersStr, ShowPoolsStr, ShowShardsStr, ShowKeyRangesStr, ShowShardingRules:
				yyVAL.str = v
			default:
				yyVAL.str = ShowUnsupportedStr
			}
		}
	case 33:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:208
		{
			switch v := string(yyDollar[1].str); v {
			case KillClientsStr:
				yyVAL.str = v
			default:
				yyVAL.str = "unsupp"
			}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
//line yacc/console/sql.y:220
		{
			yyVAL.show = &Show{Cmd: yyDollar[2].str}
		}
	case 35:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:226
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 36:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:232
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 37:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:238
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:244
		{
			yyVAL.bytes = []byte(yyDollar[1].str)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:250
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:257
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:263
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 43:
		yyDollar = yyS[yypt-11 : yypt+1]
//line yacc/console/sql.y:272
		{
			yyVAL.add = &Add{Element: &AddKeyRange{LowerBound: yyDollar[6].bytes, UpperBound: yyDollar[8].bytes, ShardID: yyDollar[11].str, KeyRangeID: yyDollar[4].str}}
		}
	case 44:
		yyDollar = yyS[yypt-6 : yypt+1]
//line yacc/console/sql.y:278
		{
			yyVAL.add = &Add{Element: &AddShardingRule{ID: yyDollar[4].str, ColNames: []string{yyDollar[6].str}}}
		}
	case 45:
		yyDollar = yyS[yypt-6 : yypt+1]
//line yacc/console/sql.y:284
		{
			yyVAL.add = &Add{Element: &AddShard{Id: yyDollar[3].str, Hosts: []string{yyDollar[6].str}}}
		}
	case 47:
		yyDollar = yyS[yypt-4 : yypt+1]
//line yacc/console/sql.y:293
		{
			yyVAL.drop = &Drop{Element: &DropShardingRule{ID: yyDollar[4].str}}
		}
	case 48:
		yyDollar = yyS[yypt-4 : yypt+1]
//line yacc/console/sql.y:299
		{
			yyVAL.drop = &Drop{Element: &DropKeyRange{KeyRangeID: yyDollar[4].str}}
		}
	case 49:
		yyDollar = yyS[yypt-4 : yypt+1]
//line yacc/console/sql.y:305
		{
			yyVAL.lock = &Lock{KeyRangeID: yyDollar[4].str}
		}
	case 50:
		yyDollar = yyS[yypt-4 : yypt+1]
//line yacc/console/sql.y:311
		{
			yyVAL.unlock = &Unlock{KeyRangeID: yyDollar[4].str}
		}
	case 51:
		yyDollar = yyS[yypt-8 : yypt+1]
//line yacc/console/sql.y:318
		{
			yyVAL.split = &SplitKeyRange{KeyRangeID: yyDollar[4].str, KeyRangeFromID: yyDollar[6].str, Border: yyDollar[8].bytes}
		}
	case 52:
		yyDollar = yyS[yypt-2 : yypt+1]
//line yacc/console/sql.y:324
		{
			yyVAL.kill = &Kill{Cmd: yyDollar[2].str}
		}
	case 53:
		yyDollar = yyS[yypt-6 : yypt+1]
//line yacc/console/sql.y:330
		{
			yyVAL.move = &MoveKeyRange{KeyRangeID: yyDollar[4].str, DestShardID: yyDollar[6].str}
		}
	case 54:
		yyDollar = yyS[yypt-6 : yypt+1]
//line yacc/console/sql.y:336
		{
			yyVAL.unite = &UniteKeyRange{KeyRangeIDL: yyDollar[4].str, KeyRangeIDR: yyDollar[5].str}
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
//line yacc/console/sql.y:342
		{
			yyVAL.listen = &Listen{addr: yyDollar[2].str}
		}
	case 56:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:348
		{
			yyVAL.shutdown = &Shutdown{}
		}
	case 57:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:356
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:362
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 59:
		yyDollar = yyS[yypt-5 : yypt+1]
//line yacc/console/sql.y:368
		{
			yyVAL.register_router = &RegisterRouter{ID: yyDollar[3].str, Addr: yyDollar[5].str}
		}
	case 60:
		yyDollar = yyS[yypt-4 : yypt+1]
//line yacc/console/sql.y:374
		{
			yyVAL.dropAll = &DropAll{Entity: EntityKeyRanges}
		}
	case 61:
		yyDollar = yyS[yypt-4 : yypt+1]
//line yacc/console/sql.y:381
		{
			yyVAL.dropAll = &DropAll{Entity: EntityShardingRule}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
//line yacc/console/sql.y:388
		{
			yyVAL.dropAll = &DropAll{Entity: EntityRouters}
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
//line yacc/console/sql.y:394
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: yyDollar[3].str}
		}
	}
	goto yystack /* stack new state and value */
}
