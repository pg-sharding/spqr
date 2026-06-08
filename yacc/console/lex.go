
//line lex.rl:1
package spqrparser

import (
    "strings"
    "strconv"
)



//line lex.go:13
const lexer_start int = 6
const lexer_first_final int = 6
const lexer_error int = 0

const lexer_en_main int = 6


//line lex.rl:15



type Lexer struct {
	data         []byte
	p, pe, cs    int
	ts, te, act  int

	result []string
}

func NewLexer(data []byte) *Lexer {
    lex := &Lexer{ 
        data: data,
        pe: len(data),
    }
    
//line lex.go:39
	{
	 lex.cs = lexer_start
	 lex.ts = 0
	 lex.te = 0
	 lex.act = 0
	}

//line lex.rl:32
    return lex
}

func ResetLexer(lex *Lexer, data []byte) {
    lex.pe = len(data)
    lex.data = data
    
//line lex.go:55
	{
	 lex.cs = lexer_start
	 lex.ts = 0
	 lex.te = 0
	 lex.act = 0
	}

//line lex.rl:39
}

func (l *Lexer) Error(msg string) {
	println(msg)
}


func (lex *Lexer) Lex(lval *yySymType) int {
    eof := lex.pe
    var tok int

    
//line lex.go:76
	{
	if ( lex.p) == ( lex.pe) {
		goto _test_eof
	}
	switch  lex.cs {
	case 6:
		goto st_case_6
	case 0:
		goto st_case_0
	case 7:
		goto st_case_7
	case 8:
		goto st_case_8
	case 1:
		goto st_case_1
	case 2:
		goto st_case_2
	case 9:
		goto st_case_9
	case 10:
		goto st_case_10
	case 11:
		goto st_case_11
	case 3:
		goto st_case_3
	case 4:
		goto st_case_4
	case 5:
		goto st_case_5
	case 12:
		goto st_case_12
	case 13:
		goto st_case_13
	case 14:
		goto st_case_14
	case 15:
		goto st_case_15
	}
	goto st_out
tr2:
//line lex.rl:97
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts + 1:lex.te - 1]); tok = IDENT; {( lex.p)++;  lex.cs = 6; goto _out }}
	goto st6
tr4:
//line lex.rl:108
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts + 1:lex.te - 1]); tok = SCONST; {( lex.p)++;  lex.cs = 6; goto _out }}
	goto st6
tr6:
//line NONE:1
	switch  lex.act {
	case 0:
	{{goto st0 }}
	case 2:
	{( lex.p) = ( lex.te) - 1
/* nothing */}
	case 7:
	{( lex.p) = ( lex.te) - 1
 lval.str = string(lex.data[lex.ts:lex.te]); tok = TEQ; {( lex.p)++;  lex.cs = 6; goto _out }}
	case 15:
	{( lex.p) = ( lex.te) - 1
 lval.str = string(lex.data[lex.ts:lex.te]); tok = TPLUS; {( lex.p)++;  lex.cs = 6; goto _out }}
	case 17:
	{( lex.p) = ( lex.te) - 1
 lval.str = string(lex.data[lex.ts:lex.te]); tok = TMUL; {( lex.p)++;  lex.cs = 6; goto _out }}
	case 18:
	{( lex.p) = ( lex.te) - 1
 lval.str = string(lex.data[lex.ts:lex.te]); tok = TLESS; {( lex.p)++;  lex.cs = 6; goto _out }}
	case 19:
	{( lex.p) = ( lex.te) - 1
 lval.str = string(lex.data[lex.ts:lex.te]); tok = TGREATER; {( lex.p)++;  lex.cs = 6; goto _out }}
	case 20:
	{( lex.p) = ( lex.te) - 1

                lval.str = string(lex.data[lex.ts:lex.te]); tok = int(OP);    
                {( lex.p)++;  lex.cs = 6; goto _out }
            }
	}
	
	goto st6
tr11:
//line lex.rl:113
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TOPENBR; {( lex.p)++;  lex.cs = 6; goto _out }}
	goto st6
tr12:
//line lex.rl:114
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TCLOSEBR; {( lex.p)++;  lex.cs = 6; goto _out }}
	goto st6
tr15:
//line lex.rl:111
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TCOMMA; {( lex.p)++;  lex.cs = 6; goto _out }}
	goto st6
tr17:
//line lex.rl:120
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TDOT; {( lex.p)++;  lex.cs = 6; goto _out }}
	goto st6
tr21:
//line lex.rl:117
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TSEMICOLON; {( lex.p)++;  lex.cs = 6; goto _out }}
	goto st6
tr26:
//line lex.rl:115
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TOPENSQBR; {( lex.p)++;  lex.cs = 6; goto _out }}
	goto st6
tr27:
//line lex.rl:116
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TCLOSESQBR; {( lex.p)++;  lex.cs = 6; goto _out }}
	goto st6
tr28:
//line lex.rl:83
 lex.te = ( lex.p)
( lex.p)--
{ /* do nothing */ }
	goto st6
tr29:
//line lex.rl:118
 lex.te = ( lex.p)
( lex.p)--
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TMINUS; {( lex.p)++;  lex.cs = 6; goto _out }}
	goto st6
tr31:
//line lex.rl:85
 lex.te = ( lex.p)
( lex.p)--
{/* nothing */}
	goto st6
tr33:
//line lex.rl:86
 lex.te = ( lex.p)
( lex.p)--
{ 
                vl, err := strconv.ParseUint(string(lex.data[lex.ts:lex.te]), 10, 64)
                if err != nil {
                    vl = 0
                    lval.uinteger = uint(vl); tok = INVALID_ICONST; {( lex.p)++;  lex.cs = 6; goto _out }    
                } else {
                    lval.uinteger = uint(vl); tok = ICONST; {( lex.p)++;  lex.cs = 6; goto _out }
                }     
            }
	goto st6
tr34:
//line lex.rl:98
 lex.te = ( lex.p)
( lex.p)--
{ 
                lval.str = string(lex.data[lex.ts:lex.te]);
                if ttype, ok := reservedWords[strings.ToLower(lval.str)]; ok {
                    lval.str = strings.ToLower(lval.str);
                    tok = ttype;
                } else {
                    tok = IDENT; 
                }
                {( lex.p)++;  lex.cs = 6; goto _out }
            }
	goto st6
	st6:
//line NONE:1
 lex.ts = 0

//line NONE:1
 lex.act = 0

		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof6
		}
	st_case_6:
//line NONE:1
 lex.ts = ( lex.p)

//line lex.go:254
		switch  lex.data[( lex.p)] {
		case 32:
			goto st7
		case 34:
			goto st1
		case 39:
			goto st2
		case 40:
			goto tr11
		case 41:
			goto tr12
		case 42:
			goto tr13
		case 43:
			goto tr14
		case 44:
			goto tr15
		case 45:
			goto st9
		case 46:
			goto tr17
		case 47:
			goto st3
		case 55:
			goto st13
		case 59:
			goto tr21
		case 60:
			goto tr22
		case 61:
			goto tr23
		case 62:
			goto tr24
		case 91:
			goto tr26
		case 92:
			goto tr10
		case 93:
			goto tr27
		case 94:
			goto tr10
		case 96:
			goto tr10
		case 124:
			goto tr10
		case 126:
			goto tr10
		}
		switch {
		case  lex.data[( lex.p)] < 48:
			switch {
			case  lex.data[( lex.p)] < 33:
				if 9 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 13 {
					goto st7
				}
			case  lex.data[( lex.p)] > 35:
				if 37 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 38 {
					goto tr10
				}
			default:
				goto tr10
			}
		case  lex.data[( lex.p)] > 51:
			switch {
			case  lex.data[( lex.p)] < 63:
				if 52 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 57 {
					goto st15
				}
			case  lex.data[( lex.p)] > 64:
				if 65 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 122 {
					goto st14
				}
			default:
				goto tr10
			}
		default:
			goto st13
		}
		goto st0
st_case_0:
	st0:
		 lex.cs = 0
		goto _out
	st7:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof7
		}
	st_case_7:
		if  lex.data[( lex.p)] == 32 {
			goto st7
		}
		if 9 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 13 {
			goto st7
		}
		goto tr28
tr10:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:125
 lex.act = 20;
	goto st8
tr13:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:121
 lex.act = 17;
	goto st8
tr14:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:119
 lex.act = 15;
	goto st8
tr22:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:122
 lex.act = 18;
	goto st8
tr23:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:110
 lex.act = 7;
	goto st8
tr24:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:123
 lex.act = 19;
	goto st8
	st8:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof8
		}
	st_case_8:
//line lex.go:397
		switch  lex.data[( lex.p)] {
		case 33:
			goto tr10
		case 35:
			goto tr10
		case 45:
			goto tr10
		case 92:
			goto tr10
		case 94:
			goto tr10
		case 96:
			goto tr10
		case 124:
			goto tr10
		case 126:
			goto tr10
		}
		switch {
		case  lex.data[( lex.p)] < 42:
			if 37 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 38 {
				goto tr10
			}
		case  lex.data[( lex.p)] > 43:
			if 60 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 64 {
				goto tr10
			}
		default:
			goto tr10
		}
		goto tr6
	st1:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof1
		}
	st_case_1:
		switch  lex.data[( lex.p)] {
		case 10:
			goto st0
		case 13:
			goto st0
		case 34:
			goto tr2
		}
		goto st1
	st2:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof2
		}
	st_case_2:
		if  lex.data[( lex.p)] == 39 {
			goto tr4
		}
		goto st2
	st9:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof9
		}
	st_case_9:
		switch  lex.data[( lex.p)] {
		case 33:
			goto tr10
		case 35:
			goto tr10
		case 45:
			goto st10
		case 92:
			goto tr10
		case 94:
			goto tr10
		case 96:
			goto tr10
		case 124:
			goto tr10
		case 126:
			goto tr10
		}
		switch {
		case  lex.data[( lex.p)] < 42:
			if 37 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 38 {
				goto tr10
			}
		case  lex.data[( lex.p)] > 43:
			if 60 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 64 {
				goto tr10
			}
		default:
			goto tr10
		}
		goto tr29
	st10:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof10
		}
	st_case_10:
		switch  lex.data[( lex.p)] {
		case 10:
			goto tr31
		case 13:
			goto tr31
		case 33:
			goto st10
		case 35:
			goto st10
		case 45:
			goto st10
		case 92:
			goto st10
		case 94:
			goto st10
		case 96:
			goto st10
		case 124:
			goto st10
		case 126:
			goto st10
		}
		switch {
		case  lex.data[( lex.p)] < 42:
			if 37 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 38 {
				goto st10
			}
		case  lex.data[( lex.p)] > 43:
			if 60 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 64 {
				goto st10
			}
		default:
			goto st10
		}
		goto st11
	st11:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof11
		}
	st_case_11:
		switch  lex.data[( lex.p)] {
		case 10:
			goto tr31
		case 13:
			goto tr31
		}
		goto st11
	st3:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof3
		}
	st_case_3:
		if  lex.data[( lex.p)] == 42 {
			goto st4
		}
		goto st0
	st4:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof4
		}
	st_case_4:
		if  lex.data[( lex.p)] == 42 {
			goto st5
		}
		goto st4
	st5:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof5
		}
	st_case_5:
		switch  lex.data[( lex.p)] {
		case 42:
			goto st5
		case 47:
			goto tr8
		}
		goto st4
tr8:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:85
 lex.act = 2;
	goto st12
	st12:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof12
		}
	st_case_12:
//line lex.go:582
		if  lex.data[( lex.p)] == 42 {
			goto st5
		}
		goto st4
	st13:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof13
		}
	st_case_13:
		switch  lex.data[( lex.p)] {
		case 36:
			goto st14
		case 95:
			goto st14
		}
		switch {
		case  lex.data[( lex.p)] < 65:
			if 48 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 57 {
				goto st13
			}
		case  lex.data[( lex.p)] > 90:
			if 97 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 122 {
				goto st14
			}
		default:
			goto st14
		}
		goto tr33
	st14:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof14
		}
	st_case_14:
		switch  lex.data[( lex.p)] {
		case 36:
			goto st14
		case 95:
			goto st14
		}
		switch {
		case  lex.data[( lex.p)] < 65:
			if 48 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 57 {
				goto st14
			}
		case  lex.data[( lex.p)] > 90:
			if 97 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 122 {
				goto st14
			}
		default:
			goto st14
		}
		goto tr34
	st15:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof15
		}
	st_case_15:
		if 48 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 57 {
			goto st15
		}
		goto tr33
	st_out:
	_test_eof6:  lex.cs = 6; goto _test_eof
	_test_eof7:  lex.cs = 7; goto _test_eof
	_test_eof8:  lex.cs = 8; goto _test_eof
	_test_eof1:  lex.cs = 1; goto _test_eof
	_test_eof2:  lex.cs = 2; goto _test_eof
	_test_eof9:  lex.cs = 9; goto _test_eof
	_test_eof10:  lex.cs = 10; goto _test_eof
	_test_eof11:  lex.cs = 11; goto _test_eof
	_test_eof3:  lex.cs = 3; goto _test_eof
	_test_eof4:  lex.cs = 4; goto _test_eof
	_test_eof5:  lex.cs = 5; goto _test_eof
	_test_eof12:  lex.cs = 12; goto _test_eof
	_test_eof13:  lex.cs = 13; goto _test_eof
	_test_eof14:  lex.cs = 14; goto _test_eof
	_test_eof15:  lex.cs = 15; goto _test_eof

	_test_eof: {}
	if ( lex.p) == eof {
		switch  lex.cs {
		case 7:
			goto tr28
		case 8:
			goto tr6
		case 9:
			goto tr29
		case 10:
			goto tr31
		case 11:
			goto tr31
		case 4:
			goto tr6
		case 5:
			goto tr6
		case 12:
			goto tr31
		case 13:
			goto tr33
		case 14:
			goto tr34
		case 15:
			goto tr33
		}
	}

	_out: {}
	}

//line lex.rl:132


    return int(tok);
}