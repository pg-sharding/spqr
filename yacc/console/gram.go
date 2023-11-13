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
	yys int
	str                    string
	byte                   byte
	bytes                  []byte
	int                    int
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
	sharding_rule          *ShardingRuleDefinition

	register_router        *RegisterRouter
	unregister_router      *UnregisterRouter
	
	split                  *SplitKeyRange
	move                   *MoveKeyRange
	unite                  *UniteKeyRange

	shutdown               *Shutdown
	listen                 *Listen

	trace                  *TraceStmt
	stoptrace              *StopTraceStmt
	
	entrieslist            []ShardingRuleEntry
	shruleEntry            ShardingRuleEntry

	sharding_rule_selector *ShardingRuleSelector
	key_range_selector     *KeyRangeSelector
	dataspace_selector     *DataspaceSelector

    colref                 ColumnRef
    where                  WhereClauseNode
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
const HARD = 57373
const SHARDING = 57374
const COLUMN = 57375
const TABLE = 57376
const HASH = 57377
const FUNCTION = 57378
const KEY = 57379
const RANGE = 57380
const DATASPACE = 57381
const SHARDS = 57382
const KEY_RANGES = 57383
const ROUTERS = 57384
const SHARD = 57385
const HOST = 57386
const SHARDING_RULES = 57387
const RULE = 57388
const COLUMNS = 57389
const VERSION = 57390
const BY = 57391
const FROM = 57392
const TO = 57393
const WITH = 57394
const UNITE = 57395
const ALL = 57396
const ADDRESS = 57397
const FOR = 57398
const CLIENT = 57399
const START = 57400
const STOP = 57401
const TRACE = 57402
const MESSAGES = 57403
const OP = 57404

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
	"HARD",
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

//line gram.y:643



//line yacctab:1
var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 192

var yyAct = [...]int{

	118, 145, 68, 115, 102, 125, 108, 124, 84, 28,
	29, 52, 51, 82, 67, 122, 83, 147, 127, 31,
	30, 35, 36, 77, 77, 21, 20, 25, 26, 27,
	32, 33, 128, 24, 77, 77, 106, 170, 77, 97,
	169, 147, 162, 153, 76, 96, 77, 80, 130, 95,
	142, 127, 86, 78, 160, 132, 34, 107, 61, 44,
	89, 22, 23, 43, 45, 128, 43, 66, 93, 94,
	46, 90, 161, 112, 110, 98, 99, 85, 79, 101,
	104, 149, 81, 77, 109, 100, 111, 113, 88, 54,
	103, 111, 105, 39, 57, 114, 119, 120, 121, 55,
	87, 59, 168, 167, 129, 42, 123, 75, 131, 74,
	133, 77, 41, 103, 40, 47, 60, 62, 53, 38,
	138, 116, 71, 72, 73, 143, 92, 50, 150, 151,
	146, 144, 70, 152, 49, 154, 48, 140, 155, 77,
	69, 64, 157, 135, 141, 158, 159, 135, 137, 136,
	146, 37, 137, 136, 156, 1, 163, 19, 18, 17,
	16, 164, 165, 15, 13, 166, 14, 9, 10, 148,
	171, 172, 126, 173, 174, 6, 5, 4, 3, 8,
	12, 11, 7, 65, 63, 58, 56, 2, 117, 139,
	134, 91,
}
var yyPact = [...]int{

	3, -1000, 106, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	27, 27, -48, -49, 24, 62, 21, 21, 137, 10,
	128, -1000, 21, 21, 21, 89, 87, -1000, -1000, -1000,
	-1000, -1000, -1000, 135, 7, 40, 30, -1000, -1000, -1000,
	-1000, -41, -53, -1000, -1000, 39, -1000, 6, 69, 34,
	-1000, 33, -1000, 118, -1000, 128, 128, -1000, -1000, -1000,
	-1000, -1, -6, -13, 135, 31, -1000, -1000, 79, 42,
	-16, 13, -55, 135, -1000, 20, 19, -1000, 64, -1000,
	135, -1000, 107, -1000, -1000, 135, 135, 135, -40, -1000,
	-1000, 56, 18, 135, -2, 128, 11, 128, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 143, 107, 133, -1000, 1,
	-1000, -1000, 128, 18, -15, -1000, 46, 135, 135, -1000,
	128, -8, 128, -1000, 107, -1000, -1000, -1000, 139, 128,
	-1000, -1000, 128, -1000, -15, -1000, -1000, 15, -1000, 36,
	-1000, -1000, -9, 128, -1000, 143, -1000, -1000, -1000, -1000,
	135, 135, 128, 82, -1000, -1000, 81, -11, -14, 135,
	135, -39, -39, -1000, -1000,
}
var yyPgo = [...]int{

	0, 191, 3, 190, 189, 188, 2, 0, 187, 186,
	89, 185, 184, 183, 182, 181, 180, 179, 178, 177,
	176, 175, 93, 114, 112, 105, 7, 5, 4, 172,
	169, 1, 168, 167, 166, 164, 163, 160, 159, 158,
	157, 155, 151,
}
var yyR1 = [...]int{

	0, 41, 42, 42, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
	8, 6, 6, 7, 3, 3, 3, 4, 4, 5,
	2, 2, 2, 1, 1, 12, 13, 14, 17, 17,
	17, 17, 17, 17, 17, 17, 18, 18, 18, 18,
	20, 20, 21, 19, 19, 19, 19, 15, 33, 22,
	23, 23, 26, 26, 27, 28, 28, 29, 29, 30,
	30, 31, 31, 24, 24, 25, 25, 32, 9, 10,
	11, 36, 16, 16, 37, 38, 35, 34, 39, 40,
	40,
}
var yyR2 = [...]int{

	0, 2, 0, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	3, 3, 3, 0, 2, 1, 1, 2, 2, 4,
	2, 4, 2, 3, 3, 4, 2, 2, 2, 2,
	4, 4, 3, 2, 2, 2, 2, 3, 2, 2,
	6, 5, 1, 2, 2, 2, 0, 2, 2, 3,
	0, 3, 0, 11, 10, 5, 4, 2, 3, 3,
	2, 6, 3, 3, 4, 4, 2, 1, 5, 3,
	3,
}
var yyChk = [...]int{

	-1000, -41, -8, -18, -19, -20, -21, -14, -17, -33,
	-32, -15, -16, -35, -34, -36, -37, -38, -39, -40,
	23, 22, 58, 59, 30, 24, 25, 26, 6, 7,
	17, 16, 27, 28, 53, 18, 19, -42, 13, -22,
	-23, -24, -25, 39, 32, 37, 43, -22, -23, -24,
	-25, 60, 60, -22, -10, 37, -9, 32, -11, 39,
	-10, 37, -10, -12, 4, -13, 57, 4, -6, 12,
	4, -10, -10, -10, 20, 20, -7, 4, 46, 38,
	-7, 52, 54, 57, 61, 38, 46, 31, 54, -7,
	38, -1, 8, -6, -6, 50, 51, 52, -7, -7,
	54, -7, -28, 34, -7, 50, 52, 44, 61, -7,
	54, -7, 54, -7, 31, -2, 14, -5, -7, -7,
	-7, -7, 55, -28, -26, -27, -29, 33, 47, -7,
	50, -6, 44, -6, -3, 4, 10, 9, -2, -4,
	4, 11, 49, -6, -26, -31, -27, 56, -30, 35,
	-7, -7, -6, 51, -6, -2, 15, -6, -6, -31,
	39, 36, 51, -6, -7, -7, -6, 21, 21, 51,
	51, -7, -7, -31, -31,
}
var yyDef = [...]int{

	0, -2, 2, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 87, 0, 0, 0, 0, 0, 1, 3, 46,
	47, 48, 49, 0, 0, 0, 0, 53, 54, 55,
	56, 0, 0, 37, 38, 0, 40, 0, 42, 0,
	58, 0, 77, 33, 35, 0, 0, 36, 86, 21,
	22, 0, 0, 0, 0, 0, 59, 23, 66, 0,
	0, 0, 0, 0, 52, 0, 0, 44, 43, 80,
	0, 57, 0, 82, 83, 0, 0, 0, 0, 89,
	90, 66, 0, 0, 0, 0, 0, 0, 50, 51,
	39, 79, 41, 78, 45, 34, 0, 0, 29, 0,
	84, 85, 0, 0, 72, 62, 70, 0, 0, 65,
	0, 0, 0, 76, 0, 24, 25, 26, 0, 0,
	27, 28, 0, 88, 72, 61, 63, 0, 64, 0,
	67, 68, 0, 0, 75, 32, 30, 31, 81, 60,
	0, 0, 0, 0, 71, 69, 0, 0, 0, 0,
	0, 72, 72, 74, 73,
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
	62,
}
var yyTok3 = [...]int{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{
}

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
		yyDollar = yyS[yypt-0:yypt+1]
//line gram.y:170
		{}
	case 3:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:171
		{}
	case 4:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:176
		{
			setParseTree(yylex, yyDollar[1].create)
		}
	case 5:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:180
		{
			setParseTree(yylex, yyDollar[1].create)
		}
	case 6:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:184
		{
			setParseTree(yylex, yyDollar[1].trace)
		}
	case 7:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:188
		{
			setParseTree(yylex, yyDollar[1].stoptrace)
		}
	case 8:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:192
		{
			setParseTree(yylex, yyDollar[1].set)
		}
	case 9:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:196
		{
			setParseTree(yylex, yyDollar[1].drop)
		}
	case 10:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:200
		{
			setParseTree(yylex, yyDollar[1].lock)
		}
	case 11:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:204
		{
			setParseTree(yylex, yyDollar[1].unlock)
		}
	case 12:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:208
		{
			setParseTree(yylex, yyDollar[1].show)
		}
	case 13:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:212
		{
			setParseTree(yylex, yyDollar[1].kill)
		}
	case 14:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:216
		{
			setParseTree(yylex, yyDollar[1].listen)
		}
	case 15:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:220
		{
			setParseTree(yylex, yyDollar[1].shutdown)
		}
	case 16:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:224
		{
			setParseTree(yylex, yyDollar[1].split)
		}
	case 17:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:228
		{
			setParseTree(yylex, yyDollar[1].move)
		}
	case 18:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:232
		{
		   setParseTree(yylex, yyDollar[1].unite)
		}
	case 19:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:236
		{
			setParseTree(yylex, yyDollar[1].register_router)
		}
	case 20:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:240
		{
			setParseTree(yylex, yyDollar[1].unregister_router)
		}
	case 21:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:245
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 22:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:249
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 23:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:254
		{
			yyVAL.str = string(yyDollar[1].str)
		}
	case 24:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:260
		{
	        yyVAL.str = yyDollar[1].str
	    }
	case 25:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:262
		{
	        yyVAL.str = "AND"
	    }
	case 26:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:264
		{
	        yyVAL.str = "OR"
	    }
	case 27:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:269
		{
	        yyVAL.str = yyDollar[1].str
	    }
	case 28:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:271
		{
	        yyVAL.str = "="
	    }
	case 29:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:277
		{
	        yyVAL.colref = ColumnRef{
	            ColName: yyDollar[1].str,
	        }
	    }
	case 30:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:285
		{
	        yyVAL. where = yyDollar[2]. where
	    }
	case 31:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:288
		{
	        yyVAL. where = WhereClauseLeaf {
	            ColRef:     yyDollar[1].colref,
				Op:         yyDollar[2].str,
	            Value:      yyDollar[3].str,
	        }
	    }
	case 32:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:296
		{
	        yyVAL. where = WhereClauseOp{
	            Op: yyDollar[2].str,
	            Left: yyDollar[1]. where,
	            Right: yyDollar[3]. where,
	        }
	    }
	case 33:
		yyDollar = yyS[yypt-0:yypt+1]
//line gram.y:306
		{
	        yyVAL. where = WhereClauseEmpty{}
	    }
	case 34:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:310
		{
	        yyVAL. where = yyDollar[2]. where
	    }
	case 35:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:317
		{
			switch v := strings.ToLower(string(yyDollar[1].str)); v {
			case DatabasesStr, RoutersStr, PoolsStr, ShardsStr,BackendConnectionsStr, KeyRangesStr, ShardingRules, ClientsStr, StatusStr, DataspacesStr, VersionStr:
				yyVAL.str = v
			default:
				yyVAL.str = UnsupportedStr
			}
		}
	case 36:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:328
		{
			switch v := string(yyDollar[1].str); v {
			case ClientStr:
				yyVAL.str = v
			default:
				yyVAL.str = "unsupp"
			}
		}
	case 37:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:339
		{
		    yyVAL.set = &Set{Element: yyDollar[2].ds}
		}
	case 38:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:346
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].key_range_selector}
		}
	case 39:
		yyDollar = yyS[yypt-4:yypt+1]
//line gram.y:351
		{
			yyVAL.drop = &Drop{Element: &KeyRangeSelector{KeyRangeID: `*`}}
		}
	case 40:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:355
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].sharding_rule_selector}
		}
	case 41:
		yyDollar = yyS[yypt-4:yypt+1]
//line gram.y:360
		{
			yyVAL.drop = &Drop{Element: &ShardingRuleSelector{ID: `*`}}
		}
	case 42:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:365
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].dataspace_selector, HardDelete = false}
		}
	case 43:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:370
		{
			yyVAL.drop = &Drop{Element: &DataspaceSelector{ID: `*`}, HardDelete = false}
		}
	case 44:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:375
		{
			yyVAL.drop = &Drop{Element: yyDollar[2].dataspace_selector, HardDelete = true}
		}
	case 45:
		yyDollar = yyS[yypt-4:yypt+1]
//line gram.y:380
		{
			yyVAL.drop = &Drop{Element: &DataspaceSelector{ID: `*`}, HardDelete = true}
		}
	case 46:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:386
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 47:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:391
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 48:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:396
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 49:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:400
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 50:
		yyDollar = yyS[yypt-4:yypt+1]
//line gram.y:407
		{
			yyVAL.trace = &TraceStmt{All: true}
		}
	case 51:
		yyDollar = yyS[yypt-4:yypt+1]
//line gram.y:410
		{
			yyVAL.trace = &TraceStmt {
				Client: yyDollar[4].str,
			}
		}
	case 52:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:418
		{
			yyVAL.stoptrace = &StopTraceStmt{}
		}
	case 53:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:425
		{
			yyVAL.create = &Create{Element: yyDollar[2].ds}
		}
	case 54:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:430
		{
			yyVAL.create = &Create{Element: yyDollar[2].sharding_rule}
		}
	case 55:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:435
		{
			yyVAL.create = &Create{Element: yyDollar[2].kr}
		}
	case 56:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:439
		{
			yyVAL.create = &Create{Element: yyDollar[2].shard}
		}
	case 57:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:446
		{
			yyVAL.show = &Show{Cmd: yyDollar[2].str, Where: yyDollar[3]. where}
		}
	case 58:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:452
		{
			yyVAL.lock = &Lock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 59:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:460
		{
			yyVAL.ds = &DataspaceDefinition{ID: yyDollar[2].str}
		}
	case 60:
		yyDollar = yyS[yypt-6:yypt+1]
//line gram.y:466
		{
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID: yyDollar[3].str, TableName: yyDollar[4].str, Entries: yyDollar[5].entrieslist, Dataspace: yyDollar[6].str}
		}
	case 61:
		yyDollar = yyS[yypt-5:yypt+1]
//line gram.y:471
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.sharding_rule = &ShardingRuleDefinition{ID:  "shrule"+str, TableName: yyDollar[3].str, Entries: yyDollar[4].entrieslist, Dataspace: yyDollar[5].str}
		}
	case 62:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:480
		{
	      yyVAL.entrieslist = make([]ShardingRuleEntry, 0)
	      yyVAL.entrieslist = append(yyVAL.entrieslist, yyDollar[1].shruleEntry)
	    }
	case 63:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:486
		{
	      yyVAL.entrieslist = append(yyDollar[1].entrieslist, yyDollar[2].shruleEntry)
	    }
	case 64:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:492
		{
			yyVAL.shruleEntry = ShardingRuleEntry{
				Column: yyDollar[1].str,
				HashFunction: yyDollar[2].str,
			}
		}
	case 65:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:501
		{
	       yyVAL.str = yyDollar[2].str
	    }
	case 66:
		yyDollar = yyS[yypt-0:yypt+1]
//line gram.y:504
		{ yyVAL.str = ""; }
	case 67:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:508
		{
			yyVAL.str = yyDollar[2].str
		}
	case 68:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:513
		{
			yyVAL.str = yyDollar[2].str
		}
	case 69:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:519
		{
			yyVAL.str = yyDollar[3].str
		}
	case 70:
		yyDollar = yyS[yypt-0:yypt+1]
//line gram.y:522
		{ yyVAL.str = ""; }
	case 71:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:525
		{
	        yyVAL.str = yyDollar[3].str
	    }
	case 72:
		yyDollar = yyS[yypt-0:yypt+1]
//line gram.y:528
		{ yyVAL.str = "default" }
	case 73:
		yyDollar = yyS[yypt-11:yypt+1]
//line gram.y:533
		{
			yyVAL.kr = &KeyRangeDefinition{LowerBound: []byte(yyDollar[5].str), UpperBound: []byte(yyDollar[7].str), ShardID: yyDollar[10].str, KeyRangeID: yyDollar[3].str, Dataspace: yyDollar[11].str}
		}
	case 74:
		yyDollar = yyS[yypt-10:yypt+1]
//line gram.y:537
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.kr = &KeyRangeDefinition{LowerBound: []byte(yyDollar[4].str), UpperBound: []byte(yyDollar[6].str), ShardID: yyDollar[9].str, KeyRangeID: "kr"+str, Dataspace: yyDollar[10].str}
		}
	case 75:
		yyDollar = yyS[yypt-5:yypt+1]
//line gram.y:548
		{
			yyVAL.shard = &ShardDefinition{Id: yyDollar[2].str, Hosts: []string{yyDollar[5].str}}
		}
	case 76:
		yyDollar = yyS[yypt-4:yypt+1]
//line gram.y:553
		{
			str, err := randomHex(6)
			if err != nil {
				panic(err)
			}
			yyVAL.shard = &ShardDefinition{Id: "shard" + str, Hosts: []string{yyDollar[4].str}}
		}
	case 77:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:564
		{
			yyVAL.unlock = &Unlock{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID}
		}
	case 78:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:570
		{
			yyVAL.sharding_rule_selector =&ShardingRuleSelector{ID: yyDollar[3].str}
		}
	case 79:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:576
		{
			yyVAL.key_range_selector = &KeyRangeSelector{KeyRangeID: yyDollar[3].str}
		}
	case 80:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:582
		{
			yyVAL.dataspace_selector = &DataspaceSelector{ID: yyDollar[2].str}
		}
	case 81:
		yyDollar = yyS[yypt-6:yypt+1]
//line gram.y:588
		{
			yyVAL.split = &SplitKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeFromID: yyDollar[4].str, Border: []byte(yyDollar[6].str)}
		}
	case 82:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:594
		{
			yyVAL.kill = &Kill{Cmd: yyDollar[2].str, Target: yyDollar[3].str}
		}
	case 83:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:597
		{
			yyVAL.kill = &Kill{Cmd: "client", Target: yyDollar[3].str}
		}
	case 84:
		yyDollar = yyS[yypt-4:yypt+1]
//line gram.y:603
		{
			yyVAL.move = &MoveKeyRange{KeyRangeID: yyDollar[2].key_range_selector.KeyRangeID, DestShardID: yyDollar[4].str}
		}
	case 85:
		yyDollar = yyS[yypt-4:yypt+1]
//line gram.y:609
		{
			yyVAL.unite = &UniteKeyRange{KeyRangeIDL: yyDollar[2].key_range_selector.KeyRangeID, KeyRangeIDR: yyDollar[4].str}
		}
	case 86:
		yyDollar = yyS[yypt-2:yypt+1]
//line gram.y:615
		{
			yyVAL.listen = &Listen{addr: yyDollar[2].str}
		}
	case 87:
		yyDollar = yyS[yypt-1:yypt+1]
//line gram.y:621
		{
			yyVAL.shutdown = &Shutdown{}
		}
	case 88:
		yyDollar = yyS[yypt-5:yypt+1]
//line gram.y:629
		{
			yyVAL.register_router = &RegisterRouter{ID: yyDollar[3].str, Addr: yyDollar[5].str}
		}
	case 89:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:635
		{
			yyVAL.unregister_router = &UnregisterRouter{ID: yyDollar[3].str}
		}
	case 90:
		yyDollar = yyS[yypt-3:yypt+1]
//line gram.y:640
		{
	        yyVAL.unregister_router = &UnregisterRouter{ID: `*`}
	    }
	}
	goto yystack /* stack new state and value */
}
