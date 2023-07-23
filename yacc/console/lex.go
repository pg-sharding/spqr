
//line lex.rl:1
package spqrparser

import (
    "strings"
)


//line lex.go:11
const lexer_start int = 7
const lexer_first_final int = 7
const lexer_error int = 0

const lexer_en_main int = 7


//line lex.rl:13



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
    
//line lex.go:37
	{
	 lex.cs = lexer_start
	 lex.ts = 0
	 lex.te = 0
	 lex.act = 0
	}

//line lex.rl:30
    return lex
}

func ResetLexer(lex *Lexer, data []byte) {
    lex.pe = len(data)
    lex.data = data
    
//line lex.go:53
	{
	 lex.cs = lexer_start
	 lex.ts = 0
	 lex.te = 0
	 lex.act = 0
	}

//line lex.rl:37
}

func (l *Lexer) Error(msg string) {
	println(msg)
}


func (lex *Lexer) Lex(lval *yySymType) int {
    eof := lex.pe
    var tok int

    
//line lex.go:74
	{
	if ( lex.p) == ( lex.pe) {
		goto _test_eof
	}
	switch  lex.cs {
	case 7:
		goto st_case_7
	case 0:
		goto st_case_0
	case 8:
		goto st_case_8
	case 9:
		goto st_case_9
	case 1:
		goto st_case_1
	case 2:
		goto st_case_2
	case 3:
		goto st_case_3
	case 10:
		goto st_case_10
	case 11:
		goto st_case_11
	case 12:
		goto st_case_12
	case 4:
		goto st_case_4
	case 5:
		goto st_case_5
	case 6:
		goto st_case_6
	case 13:
		goto st_case_13
	case 14:
		goto st_case_14
	case 15:
		goto st_case_15
	case 16:
		goto st_case_16
	}
	goto st_out
tr2:
//line lex.rl:81
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts + 1:lex.te - 1]); tok = IDENT; {( lex.p)++;  lex.cs = 7; goto _out }}
	goto st7
tr4:
//line lex.rl:91
 lex.te = ( lex.p)+1
{ lval.str = string(lex.data[lex.ts + 1:lex.te - 1]); tok = SCONST; {( lex.p)++;  lex.cs = 7; goto _out }}
	goto st7
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
 lval.str = string(lex.data[lex.ts:lex.te]); tok = TEQ; {( lex.p)++;  lex.cs = 7; goto _out }}
	case 8:
	{( lex.p) = ( lex.te) - 1

                lval.str = string(lex.data[lex.ts:lex.te]); tok = int(OP);    
                {( lex.p)++;  lex.cs = 7; goto _out }
            }
	}
	
	goto st7
tr18:
//line lex.rl:76
 lex.te = ( lex.p)
( lex.p)--
{ /* do nothing */ }
	goto st7
tr19:
//line lex.rl:95
 lex.te = ( lex.p)
( lex.p)--
{
                lval.str = string(lex.data[lex.ts:lex.te]); tok = int(OP);    
                {( lex.p)++;  lex.cs = 7; goto _out }
            }
	goto st7
tr21:
//line lex.rl:78
 lex.te = ( lex.p)
( lex.p)--
{/* nothing */}
	goto st7
tr23:
//line lex.rl:79
 lex.te = ( lex.p)
( lex.p)--
{ lval.str = string(lex.data[lex.ts:lex.te]); tok = SCONST; {( lex.p)++;  lex.cs = 7; goto _out }}
	goto st7
tr24:
//line lex.rl:82
 lex.te = ( lex.p)
( lex.p)--
{ 
                
                lval.str = strings.ToLower(string(lex.data[lex.ts:lex.te]));
                if ttype, ok := reservedWords[lval.str]; ok {
                    tok = ttype;
                } else {
                    tok = IDENT; 
                }
                {( lex.p)++;  lex.cs = 7; goto _out }}
	goto st7
	st7:
//line NONE:1
 lex.ts = 0

//line NONE:1
 lex.act = 0

		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof7
		}
	st_case_7:
//line NONE:1
 lex.ts = ( lex.p)

//line lex.go:201
		switch  lex.data[( lex.p)] {
		case 32:
			goto st8
		case 34:
			goto st1
		case 39:
			goto st3
		case 45:
			goto st10
		case 47:
			goto st4
		case 55:
			goto st14
		case 61:
			goto tr16
		case 92:
			goto tr10
		case 95:
			goto st15
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
					goto st8
				}
			case  lex.data[( lex.p)] > 35:
				switch {
				case  lex.data[( lex.p)] > 38:
					if 42 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 43 {
						goto tr10
					}
				case  lex.data[( lex.p)] >= 37:
					goto tr10
				}
			default:
				goto tr10
			}
		case  lex.data[( lex.p)] > 51:
			switch {
			case  lex.data[( lex.p)] < 65:
				switch {
				case  lex.data[( lex.p)] > 57:
					if 60 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 64 {
						goto tr10
					}
				case  lex.data[( lex.p)] >= 52:
					goto st16
				}
			case  lex.data[( lex.p)] > 90:
				switch {
				case  lex.data[( lex.p)] > 96:
					if 97 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 122 {
						goto st15
					}
				case  lex.data[( lex.p)] >= 94:
					goto tr10
				}
			default:
				goto st15
			}
		default:
			goto st14
		}
		goto st0
st_case_0:
	st0:
		 lex.cs = 0
		goto _out
	st8:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof8
		}
	st_case_8:
		if  lex.data[( lex.p)] == 32 {
			goto st8
		}
		if 9 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 13 {
			goto st8
		}
		goto tr18
tr10:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:95
 lex.act = 8;
	goto st9
tr16:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:93
 lex.act = 7;
	goto st9
	st9:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof9
		}
	st_case_9:
//line lex.go:307
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
		case 55:
			goto st2
		case 95:
			goto st2
		}
		switch {
		case  lex.data[( lex.p)] < 65:
			if 48 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 51 {
				goto st2
			}
		case  lex.data[( lex.p)] > 90:
			if 97 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 122 {
				goto st2
			}
		default:
			goto st2
		}
		goto st0
	st2:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof2
		}
	st_case_2:
		switch  lex.data[( lex.p)] {
		case 34:
			goto tr2
		case 36:
			goto st2
		case 95:
			goto st2
		}
		switch {
		case  lex.data[( lex.p)] < 65:
			if 48 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 57 {
				goto st2
			}
		case  lex.data[( lex.p)] > 90:
			if 97 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 122 {
				goto st2
			}
		default:
			goto st2
		}
		goto st0
	st3:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof3
		}
	st_case_3:
		if  lex.data[( lex.p)] == 39 {
			goto tr4
		}
		goto st3
	st10:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof10
		}
	st_case_10:
		switch  lex.data[( lex.p)] {
		case 33:
			goto tr10
		case 35:
			goto tr10
		case 45:
			goto st11
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
		goto tr19
	st11:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof11
		}
	st_case_11:
		switch  lex.data[( lex.p)] {
		case 10:
			goto tr21
		case 13:
			goto tr21
		case 33:
			goto st11
		case 35:
			goto st11
		case 45:
			goto st11
		case 92:
			goto st11
		case 94:
			goto st11
		case 96:
			goto st11
		case 124:
			goto st11
		case 126:
			goto st11
		}
		switch {
		case  lex.data[( lex.p)] < 42:
			if 37 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 38 {
				goto st11
			}
		case  lex.data[( lex.p)] > 43:
			if 60 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 64 {
				goto st11
			}
		default:
			goto st11
		}
		goto st12
	st12:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof12
		}
	st_case_12:
		switch  lex.data[( lex.p)] {
		case 10:
			goto tr21
		case 13:
			goto tr21
		}
		goto st12
	st4:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof4
		}
	st_case_4:
		if  lex.data[( lex.p)] == 42 {
			goto st5
		}
		goto st0
	st5:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof5
		}
	st_case_5:
		if  lex.data[( lex.p)] == 42 {
			goto st6
		}
		goto st5
	st6:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof6
		}
	st_case_6:
		switch  lex.data[( lex.p)] {
		case 42:
			goto st6
		case 47:
			goto tr8
		}
		goto st5
tr8:
//line NONE:1
 lex.te = ( lex.p)+1

//line lex.rl:78
 lex.act = 2;
	goto st13
	st13:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof13
		}
	st_case_13:
//line lex.go:528
		if  lex.data[( lex.p)] == 42 {
			goto st6
		}
		goto st5
	st14:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof14
		}
	st_case_14:
		switch  lex.data[( lex.p)] {
		case 36:
			goto st15
		case 95:
			goto st15
		}
		switch {
		case  lex.data[( lex.p)] < 65:
			if 48 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 57 {
				goto st14
			}
		case  lex.data[( lex.p)] > 90:
			if 97 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 122 {
				goto st15
			}
		default:
			goto st15
		}
		goto tr23
	st15:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof15
		}
	st_case_15:
		switch  lex.data[( lex.p)] {
		case 36:
			goto st15
		case 95:
			goto st15
		}
		switch {
		case  lex.data[( lex.p)] < 65:
			if 48 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 57 {
				goto st15
			}
		case  lex.data[( lex.p)] > 90:
			if 97 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 122 {
				goto st15
			}
		default:
			goto st15
		}
		goto tr24
	st16:
		if ( lex.p)++; ( lex.p) == ( lex.pe) {
			goto _test_eof16
		}
	st_case_16:
		if 48 <=  lex.data[( lex.p)] &&  lex.data[( lex.p)] <= 57 {
			goto st16
		}
		goto tr23
	st_out:
	_test_eof7:  lex.cs = 7; goto _test_eof
	_test_eof8:  lex.cs = 8; goto _test_eof
	_test_eof9:  lex.cs = 9; goto _test_eof
	_test_eof1:  lex.cs = 1; goto _test_eof
	_test_eof2:  lex.cs = 2; goto _test_eof
	_test_eof3:  lex.cs = 3; goto _test_eof
	_test_eof10:  lex.cs = 10; goto _test_eof
	_test_eof11:  lex.cs = 11; goto _test_eof
	_test_eof12:  lex.cs = 12; goto _test_eof
	_test_eof4:  lex.cs = 4; goto _test_eof
	_test_eof5:  lex.cs = 5; goto _test_eof
	_test_eof6:  lex.cs = 6; goto _test_eof
	_test_eof13:  lex.cs = 13; goto _test_eof
	_test_eof14:  lex.cs = 14; goto _test_eof
	_test_eof15:  lex.cs = 15; goto _test_eof
	_test_eof16:  lex.cs = 16; goto _test_eof

	_test_eof: {}
	if ( lex.p) == eof {
		switch  lex.cs {
		case 8:
			goto tr18
		case 9:
			goto tr6
		case 10:
			goto tr19
		case 11:
			goto tr21
		case 12:
			goto tr21
		case 5:
			goto tr6
		case 6:
			goto tr6
		case 13:
			goto tr21
		case 14:
			goto tr23
		case 15:
			goto tr24
		case 16:
			goto tr23
		}
	}

	_out: {}
	}

//line lex.rl:102


    return int(tok);
}