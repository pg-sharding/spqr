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
	sh_col            *ShardingColumn
	register_router   *RegisterRouter
	unregister_router *UnregisterRouter
	kill              *Kill
	drop              *Drop
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
const CREATE = 57361
const ADD = 57362
const DROP = 57363
const LOCK = 57364
const UNLOCK = 57365
const SPLIT = 57366
const MOVE = 57367
const SHARDING = 57368
const COLUMN = 57369
const KEY = 57370
const RANGE = 57371
const SHARDS = 57372
const KEY_RANGES = 57373
const ROUTERS = 57374
const BY = 57375
const FROM = 57376
const TO = 57377
const WITH = 57378
const UNITE = 57379

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
	"BY",
	"FROM",
	"TO",
	"WITH",
	"UNITE",
	"';'",
}
var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

//line yacc/console/sql.y:337

//line yacctab:1
var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 97

var yyAct = [...]int{

	75, 80, 90, 67, 22, 23, 36, 87, 86, 85,
	94, 72, 25, 24, 29, 30, 71, 17, 31, 32,
	33, 34, 26, 27, 40, 45, 70, 43, 42, 41,
	69, 64, 63, 62, 60, 28, 59, 58, 57, 54,
	53, 52, 61, 37, 39, 56, 44, 46, 47, 55,
	81, 76, 91, 68, 74, 66, 51, 35, 1, 65,
	50, 73, 16, 15, 77, 78, 14, 13, 49, 79,
	12, 82, 83, 84, 10, 11, 20, 6, 21, 7,
	19, 5, 88, 18, 4, 3, 89, 9, 92, 8,
	48, 93, 38, 2, 95, 0, 96,
}
var yyPact = [...]int{

	-2, -1000, -32, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 17, -1000, -1000,
	-1000, -1000, 16, 16, 52, -1000, 13, 12, 11, 31,
	27, 10, 9, 8, 6, -1000, -1000, 15, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 4, 3, 2, 51, 49, 1, -3, -13,
	-18, 50, 47, 47, 47, 49, -1000, -1000, -1000, 46,
	47, 47, 47, -1000, -1000, -25, -1000, -27, -29, -1000,
	46, -1000, -1000, -1000, -1000, 47, 48, 47, 48, -23,
	-1000, -1000, -1000, 47, 46, -1000, -1000,
}
var yyPgo = [...]int{

	0, 93, 92, 90, 89, 87, 85, 84, 83, 81,
	80, 79, 78, 77, 76, 75, 74, 70, 67, 66,
	63, 62, 44, 61, 2, 60, 1, 0, 3, 59,
	58, 57,
}
var yyR1 = [...]int{

	0, 30, 31, 31, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 22, 22,
	22, 22, 22, 22, 22, 22, 2, 3, 4, 23,
	26, 6, 27, 24, 25, 9, 13, 7, 11, 8,
	10, 14, 12, 17, 5, 18, 19, 16, 15, 29,
	28, 20, 21,
}
var yyR2 = [...]int{

	0, 2, 0, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 2, 1,
	1, 4, 1, 1, 1, 1, 1, 1, 1, 7,
	4, 4, 4, 8, 2, 6, 6, 2, 1, 1,
	1, 4, 3,
}
var yyChk = [...]int{

	-1000, -30, -1, -6, -7, -9, -13, -11, -4, -5,
	-16, -15, -17, -18, -19, -20, -21, 19, -8, -10,
	-14, -12, 6, 7, 15, 14, 24, 25, 37, 16,
	17, 20, 21, 22, 23, -31, 38, 26, -2, -22,
	8, 13, 12, 11, 30, 9, 31, 32, -3, -22,
	-25, 4, 28, 28, 28, 18, 18, 28, 28, 28,
	28, 27, 29, 29, 29, -29, 4, -28, 4, 29,
	29, 29, 29, -23, 4, -27, 4, -27, -27, -28,
	-26, 4, -27, -27, -27, 34, 35, 36, -26, -27,
	-24, 4, -27, -24, 33, -27, -26,
}
var yyDef = [...]int{

	0, -2, 2, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 15, 16, 17, 0, 37, 35,
	36, 38, 0, 0, 0, 48, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 1, 3, 0, 28, 26,
	18, 19, 20, 21, 22, 23, 24, 25, 44, 27,
	47, 34, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 49, 52, 50, 0,
	0, 0, 0, 31, 29, 0, 32, 0, 0, 51,
	0, 30, 40, 41, 42, 0, 0, 0, 0, 0,
	45, 33, 46, 0, 0, 39, 43,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 38,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37,
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
//line yacc/console/sql.y:97
		{
		}
	case 3:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:98
		{
		}
	case 4:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:103
		{
			setParseTree(yylex, yyDollar[1].sh_col)
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:107
		{
			setParseTree(yylex, yyDollar[1].kr)
		}
	case 6:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:111
		{
			setParseTree(yylex, yyDollar[1].drop)
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:115
		{
			setParseTree(yylex, yyDollar[1].lock)
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:119
		{
			setParseTree(yylex, yyDollar[1].unlock)
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:123
		{
			setParseTree(yylex, yyDollar[1].show)
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:127
		{
			setParseTree(yylex, yyDollar[1].kill)
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:131
		{
			setParseTree(yylex, yyDollar[1].listen)
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:135
		{
			setParseTree(yylex, yyDollar[1].shutdown)
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:139
		{
			setParseTree(yylex, yyDollar[1].split)
		}
	case 14:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:143
		{
			setParseTree(yylex, yyDollar[1].move)
		}
	case 15:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:147
		{
			setParseTree(yylex, yyDollar[1].unite)
		}
	case 16:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:151
		{
			setParseTree(yylex, yyDollar[1].register_router)
		}
	case 17:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:155
		{
			setParseTree(yylex, yyDollar[1].unregister_router)
		}
	case 26:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:171
		{
			switch v := string(yyDollar[1].str); v {
			case ShowDatabasesStr, ShowRoutersStr, ShowPoolsStr, ShowShardsStr, ShowKeyRangesStr, ShowShardingColumns:
				yyVAL.str = v
			default:
				yyVAL.str = ShowUnsupportedStr
			}
		}
	case 27:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:182
		{
			switch v := string(yyDollar[1].str); v {
			case KillClientsStr:
				yyVAL.str = v
			default:
				yyVAL.str = "unsupp"
			}
		}
	case 28:
		yyDollar = yyS[yypt-2 : yypt+1]
//line yacc/console/sql.y:194
		{
			yyVAL.show = &Show{Cmd: yyDollar[2].str}
		}
	case 29:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:201
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 30:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:207
		{
			yyVAL.bytes = []byte(yyDollar[1].str)
		}
	case 31:
		yyDollar = yyS[yypt-4 : yypt+1]
//line yacc/console/sql.y:213
		{
			yyVAL.sh_col = &ShardingColumn{ColName: yyDollar[4].str}
		}
	case 32:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:219
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 33:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:226
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 34:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:232
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 39:
		yyDollar = yyS[yypt-7 : yypt+1]
//line yacc/console/sql.y:251
		{
			yyVAL.kr = &AddKeyRange{LowerBound: yyDollar[4].bytes, UpperBound: yyDollar[5].bytes, ShardID: yyDollar[6].str, KeyRangeID: yyDollar[7].str}
		}
	case 40:
		yyDollar = yyS[yypt-4 : yypt+1]
//line yacc/console/sql.y:257
		{
			yyVAL.drop = &Drop{KeyRangeID: yyDollar[4].str}
		}
	case 41:
		yyDollar = yyS[yypt-4 : yypt+1]
//line yacc/console/sql.y:263
		{
			yyVAL.lock = &Lock{KeyRangeID: yyDollar[4].str}
		}
	case 42:
		yyDollar = yyS[yypt-4 : yypt+1]
//line yacc/console/sql.y:269
		{
			yyVAL.unlock = &Unlock{KeyRangeID: yyDollar[4].str}
		}
	case 43:
		yyDollar = yyS[yypt-8 : yypt+1]
//line yacc/console/sql.y:276
		{
			yyVAL.split = &SplitKeyRange{KeyRangeID: yyDollar[4].str, KeyRangeFromID: yyDollar[6].str, Border: yyDollar[8].bytes}
		}
	case 44:
		yyDollar = yyS[yypt-2 : yypt+1]
//line yacc/console/sql.y:282
		{
			yyVAL.kill = &Kill{Cmd: yyDollar[2].str}
		}
	case 45:
		yyDollar = yyS[yypt-6 : yypt+1]
//line yacc/console/sql.y:288
		{
			yyVAL.move = &MoveKeyRange{KeyRangeID: yyDollar[4].str, DestShardID: yyDollar[6].str}
		}
	case 46:
		yyDollar = yyS[yypt-6 : yypt+1]
//line yacc/console/sql.y:294
		{
			yyVAL.unite = &UniteKeyRange{KeyRangeIDL: yyDollar[4].str, KeyRangeIDR: yyDollar[5].str}
		}
	case 47:
		yyDollar = yyS[yypt-2 : yypt+1]
//line yacc/console/sql.y:300
		{
			yyVAL.listen = &Listen{addr: yyDollar[2].str}
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:306
		{
			yyVAL.shutdown = &Shutdown{}
		}
	case 49:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:314
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
//line yacc/console/sql.y:320
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 51:
		yyDollar = yyS[yypt-4 : yypt+1]
//line yacc/console/sql.y:326
		{
			yyVAL.register_router = &RegisterRouter{Addr: yyDollar[3].str, ID: yyDollar[4].str}
		}
	case 52:
		yyDollar = yyS[yypt-3 : yypt+1]
//line yacc/console/sql.y:332
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: yyDollar[3].str}
		}
	}
	goto yystack /* stack new state and value */
}
