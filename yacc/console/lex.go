
//line lex.rl:1
package spqrparser

import (
    "strings"
    "strconv"
)



//line lex.go:11
const lexer_start int = 4
const lexer_first_final int = 4
const lexer_error int = 0

const lexer_en_main int = 4


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
    
//line lex.go:35
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
    
//line lex.go:49
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

    
//line lex.go:68
	{
	if ( lex.p) == ( lex.pe) {
		goto _test_eof
	}
	switch  lex.cs {
	case 4:
		goto st_case_4
	case 0:
		goto st_case_0
	case 5:
		goto st_case_5
	case 6:
		goto st_case_6
	case 7:
		goto st_case_7
	case 8:
		goto st_case_8
	case 1:
		goto st_case_1
	case 9:
		goto st_case_9
	case 10:
		goto st_case_10
	case 11:
		goto st_case_11
	case 12:
		goto st_case_12
	case 2:
		goto st_case_2
	case 3:
		goto st_case_3
	case 13:
		goto st_case_13
	case 14:
		goto st_case_14
	}
	goto st_out
tr1:
//line lex.rl:102
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts + 1:lex.te - 1]); tok = SCONST; {( lex.p)++;  lex.cs = 4; goto _out }}
	goto st4
tr2:
//line NONE:1
	switch  lex.act {
	case 2:
	{( lex.p) = ( lex.te) - 1
/* nothing */}
	case 4:
	{( lex.p) = ( lex.te) - 1
 lval.str = string(lex.data[lex.ts + 1:lex.te - 1]); tok = IDENT; {( lex.p)++;  lex.cs = 4; goto _out }}
	case 5:
	{( lex.p) = ( lex.te) - 1
 
                lval.str = string(lex.data[lex.ts:lex.te]);
                if ttype, ok := reservedWords[strings.ToLower(lval.str)]; ok {
                    lval.str = strings.ToLower(lval.str);
                    tok = ttype;
                } else {
                    tok = IDENT; 
                }
                {( lex.p)++;  lex.cs = 4; goto _out }
            }
	case 7:
	{( lex.p) = ( lex.te) - 1
 lval.str = string(lex.data[lex.ts:lex.te]); tok = TEQ; {( lex.p)++;  lex.cs = 4; goto _out }}
	case 15:
	{( lex.p) = ( lex.te) - 1
 lval.str = string(lex.data[lex.ts:lex.te]); tok = TPLUS; {( lex.p)++;  lex.cs = 4; goto _out }}
	case 17:
	{( lex.p) = ( lex.te) - 1

                lval.str = string(lex.data[lex.ts:lex.te]); tok = int(OP);    
                {( lex.p)++;  lex.cs = 4; goto _out }
            }
	}
	
	goto st4
tr11:
//line lex.rl:107
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TOPENBR; {( lex.p)++;  lex.cs = 4; goto _out }}
	goto st4
tr12:
//line lex.rl:108
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TCLOSEBR; {( lex.p)++;  lex.cs = 4; goto _out }}
	goto st4
tr14:
//line lex.rl:105
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TCOMMA; {( lex.p)++;  lex.cs = 4; goto _out }}
	goto st4
tr16:
//line lex.rl:114
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TDOT; {( lex.p)++;  lex.cs = 4; goto _out }}
	goto st4
tr19:
//line lex.rl:111
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TSEMICOLON; {( lex.p)++;  lex.cs = 4; goto _out }}
	goto st4
tr21:
//line lex.rl:109
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TOPENSQBR; {( lex.p)++;  lex.cs = 4; goto _out }}
	goto st4
tr22:
//line lex.rl:110
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TCLOSESQBR; {( lex.p)++;  lex.cs = 4; goto _out }}
	goto st4
tr23:
//line lex.rl:77
 lex.te = ( lex.p)
( lex.p)--
{ /* do nothing */ }
	goto st4
tr25:
//line lex.rl:92
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
                {( lex.p)++;  lex.cs = 4; goto _out }
            }
	goto st4
tr26:
//line lex.rl:112
 lex.te = ( lex.p)
( lex.p)--
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = TMINUS; {( lex.p)++;  lex.cs = 4; goto _out }}
	goto st4
tr28:
//line lex.rl:79
 lex.te = ( lex.p)
( lex.p)--
{/* nothing */}
	goto st4
tr30:
//line lex.rl:80
 lex.te = ( lex.p)
( lex.p)--
{ 
                vl, err := strconv.ParseUint(string(lex.data[lex.ts:lex.te]), 10, 64)
                if err != nil {
                    vl = 0
                    lval.uinteger = uint(vl); tok = INVALID_ICONST; {( lex.p)++;  lex.cs = 4; goto _out }    
                } else {
                    lval.uinteger = uint(vl); tok = ICONST; {( lex.p)++;  lex.cs = 4; goto _out }
                }     
            }
	goto st4
	st4:
//line NONE:1
 lex.ts = 0

		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof4
		}
	st_case_4:
//line NONE:1
 lex.ts = ( lex.p)

//line lex.go:240
		switch  lex.data[( lex.p)] {
		case 32:
			goto st5
		case 34:
			goto tr9
		case 36:
			goto st8
		case 39:
			goto st1
		case 40:
			goto tr11
		case 41:
			goto tr12
		case 43:
			goto tr13
		case 44:
			goto tr14
		case 45:
			goto st9
		case 46:
			goto tr16
		case 47:
			goto tr17
		case 58:
			goto st8
		case 59:
			goto tr19
		case 61:
			goto tr20
		case 91:
			goto tr21
		case 92:
			goto tr8
		case 93:
			goto tr22
		case 94:
			goto tr8
		case 96:
			goto tr8
		case 124:
			goto tr8
		case 126:
			goto tr8
		}
		switch {
		case  lex.data[( lex.p)] < 48:
			switch {
			case  lex.data[( lex.p)] > 13:
				if 33 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 42 {
					goto tr8
				}
			case  lex.data[( lex.p)] >= 9:
				goto st5
			}
		case  lex.data[( lex.p)] > 57:
			switch {
			case  lex.data[( lex.p)] > 64:
				if 65 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 125 {
					goto st8
				}
			case  lex.data[( lex.p)] >= 60:
				goto tr8
			}
		default:
			goto st14
		}
		goto st0
st_case_0:
	st0:
		 lex.cs = 0
		goto _out
	st5:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof5
		}
	st_case_5:
		if  lex.data[( lex.p)] == 32 {
			goto st5
		}
		if 9 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 13 {
			goto st5
		}
		goto tr23
tr8:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:116
 lex.act = 17;
	goto st6
tr13:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:113
 lex.act = 15;
	goto st6
tr20:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:104
 lex.act = 7;
	goto st6
	st6:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof6
		}
	st_case_6:
//line lex.go:350
		switch  lex.data[( lex.p)] {
		case 33:
			goto tr8
		case 35:
			goto tr8
		case 45:
			goto tr8
		case 92:
			goto tr8
		case 94:
			goto tr8
		case 96:
			goto tr8
		case 124:
			goto tr8
		case 126:
			goto tr8
		}
		switch {
		case  lex.data[( lex.p)] < 42:
			if 37 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 38 {
				goto tr8
			}
		case  lex.data[( lex.p)] > 43:
			if 60 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 64 {
				goto tr8
			}
		default:
			goto tr8
		}
		goto tr2
tr9:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:92
 lex.act = 5;
	goto st7
tr24:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:91
 lex.act = 4;
	goto st7
	st7:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof7
		}
	st_case_7:
//line lex.go:401
		switch  lex.data[( lex.p)] {
		case 34:
			goto tr24
		case 36:
			goto tr9
		case 95:
			goto tr9
		case 125:
			goto tr9
		}
		switch {
		case  lex.data[( lex.p)] < 65:
			if 47 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 58 {
				goto tr9
			}
		case  lex.data[( lex.p)] > 90:
			if 97 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 123 {
				goto tr9
			}
		default:
			goto tr9
		}
		goto tr2
	st8:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof8
		}
	st_case_8:
		switch  lex.data[( lex.p)] {
		case 34:
			goto st8
		case 36:
			goto st8
		case 95:
			goto st8
		case 125:
			goto st8
		}
		switch {
		case  lex.data[( lex.p)] < 65:
			if 47 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 58 {
				goto st8
			}
		case  lex.data[( lex.p)] > 90:
			if 97 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 123 {
				goto st8
			}
		default:
			goto st8
		}
		goto tr25
	st1:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof1
		}
	st_case_1:
		if  lex.data[( lex.p)] == 39 {
			goto tr1
		}
		goto st1
	st9:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof9
		}
	st_case_9:
		switch  lex.data[( lex.p)] {
		case 33:
			goto tr8
		case 35:
			goto tr8
		case 45:
			goto st10
		case 92:
			goto tr8
		case 94:
			goto tr8
		case 96:
			goto tr8
		case 124:
			goto tr8
		case 126:
			goto tr8
		}
		switch {
		case  lex.data[( lex.p)] < 42:
			if 37 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 38 {
				goto tr8
			}
		case  lex.data[( lex.p)] > 43:
			if 60 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 64 {
				goto tr8
			}
		default:
			goto tr8
		}
		goto tr26
	st10:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof10
		}
	st_case_10:
		switch  lex.data[( lex.p)] {
		case 10:
			goto tr28
		case 13:
			goto tr28
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
			goto tr28
		case 13:
			goto tr28
		}
		goto st11
tr17:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:92
 lex.act = 5;
	goto st12
	st12:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof12
		}
	st_case_12:
//line lex.go:562
		switch  lex.data[( lex.p)] {
		case 34:
			goto st8
		case 36:
			goto st8
		case 42:
			goto st2
		case 95:
			goto st8
		case 125:
			goto st8
		}
		switch {
		case  lex.data[( lex.p)] < 65:
			if 47 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 58 {
				goto st8
			}
		case  lex.data[( lex.p)] > 90:
			if 97 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 123 {
				goto st8
			}
		default:
			goto st8
		}
		goto tr25
	st2:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof2
		}
	st_case_2:
		if  lex.data[( lex.p)] == 42 {
			goto st3
		}
		goto st2
	st3:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof3
		}
	st_case_3:
		switch  lex.data[( lex.p)] {
		case 42:
			goto st3
		case 47:
			goto tr5
		}
		goto st2
tr5:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:79
 lex.act = 2;
	goto st13
	st13:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof13
		}
	st_case_13:
//line lex.go:621
		if  lex.data[( lex.p)] == 42 {
			goto st3
		}
		goto st2
	st14:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof14
		}
	st_case_14:
		switch  lex.data[( lex.p)] {
		case 34:
			goto st8
		case 36:
			goto st8
		case 47:
			goto st8
		case 58:
			goto st8
		case 95:
			goto st8
		case 125:
			goto st8
		}
		switch {
		case  lex.data[( lex.p)] < 65:
			if 48 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 57 {
				goto st14
			}
		case  lex.data[( lex.p)] > 90:
			if 97 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 123 {
				goto st8
			}
		default:
			goto st8
		}
		goto tr30
	st_out:
	_test_eof4:  lex.cs = 4; goto _test_eof
	_test_eof5:  lex.cs = 5; goto _test_eof
	_test_eof6:  lex.cs = 6; goto _test_eof
	_test_eof7:  lex.cs = 7; goto _test_eof
	_test_eof8:  lex.cs = 8; goto _test_eof
	_test_eof1:  lex.cs = 1; goto _test_eof
	_test_eof9:  lex.cs = 9; goto _test_eof
	_test_eof10:  lex.cs = 10; goto _test_eof
	_test_eof11:  lex.cs = 11; goto _test_eof
	_test_eof12:  lex.cs = 12; goto _test_eof
	_test_eof2:  lex.cs = 2; goto _test_eof
	_test_eof3:  lex.cs = 3; goto _test_eof
	_test_eof13:  lex.cs = 13; goto _test_eof
	_test_eof14:  lex.cs = 14; goto _test_eof

	_test_eof: {}
	if ( lex.p) == eof {
		switch  lex.cs {
		case 5:
			goto tr23
		case 6:
			goto tr2
		case 7:
			goto tr2
		case 8:
			goto tr25
		case 9:
			goto tr26
		case 10:
			goto tr28
		case 11:
			goto tr28
		case 12:
			goto tr25
		case 2:
			goto tr2
		case 3:
			goto tr2
		case 13:
			goto tr28
		case 14:
			goto tr30
		}
	}

	_out: {}
	}

//line lex.rl:123


    return int(tok);
}