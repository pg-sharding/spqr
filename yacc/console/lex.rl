//nolint:all
package spqrparser

import (
    "strings"
    "strconv"
)


%%{ 
    machine lexer;
    write data;
    access lex.;
    variable p lex.p;
    variable pe lex.pe;
}%%


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
    %% write init;
    return lex
}

func ResetLexer(lex *Lexer, data []byte) {
    lex.pe = len(data)
    lex.data = data
    %% write init;
}

func (l *Lexer) Error(msg string) {
	println(msg)
}


func (lex *Lexer) Lex(lval *yySymType) int {
    eof := lex.pe
    var tok int

    %%{

        op_chars	=	( '~' | '!' | '@' | '#' | '^' | '&' | '|' | '`' | '?' | '+' | '*' | '\\' | '%' | '<' | '>' | '=' | '-' ) ;

        sconst = '\'' (any-'\'')* '\'';
        # not equal, minus, brackers, etc
        identifier	=	(print - space - op_chars - '\'' - ';' - ',' - '(' - ')' - '.')*;

        qidentifier	=	'"' identifier '"';

        horiz_space	= [ \t\f];
        newline		=	[\n\r];
        non_newline	=	[^\n\r];

        sql_comment = '-''-' non_newline*;
        c_style_comment = '/''*' (any - '*''/')* '*''/';
        comment		= sql_comment | c_style_comment;


        whitespace = space+;

        operator	=	op_chars+;

        integer = digit+;

        
        main := |*
            whitespace => { /* do nothing */ };
            # integer const is string const 
            comment => {/* nothing */};
            integer =>  { 
                vl, err := strconv.ParseUint(string(lex.data[lex.ts:lex.te]), 10, 64)
                if err != nil {
                    vl = 0
                    lval.uinteger = uint(vl); tok = INVALID_ICONST; fbreak;    
                } else {
                    lval.uinteger = uint(vl); tok = ICONST; fbreak;
                }     
            };


            qidentifier      => { lval.str = string(lex.data[lex.ts + 1:lex.te - 1]); tok = IDENT; fbreak;};
            identifier      => { 
                lval.str = string(lex.data[lex.ts:lex.te]);
                if ttype, ok := reservedWords[strings.ToLower(lval.str)]; ok {
                    tok = ttype;
                } else {
                    tok = IDENT; 
                }
                fbreak;
            };
            sconst => { lval.str = string(lex.data[lex.ts + 1:lex.te - 1]); tok = SCONST; fbreak;};

            '=' => { lval.str = string(lex.data[lex.ts:lex.te]); tok = TEQ; fbreak;};
            ',' => { lval.str = string(lex.data[lex.ts:lex.te]); tok = TCOMMA; fbreak;};

            '(' => { lval.str = string(lex.data[lex.ts:lex.te]); tok = TOPENBR; fbreak;};
            ')' => { lval.str = string(lex.data[lex.ts:lex.te]); tok = TCLOSEBR; fbreak;};
            ';' => { lval.str = string(lex.data[lex.ts:lex.te]); tok = TSEMICOLON; fbreak;};
            '-' => { lval.str = string(lex.data[lex.ts:lex.te]); tok = TMINUS; fbreak;};
            '+' => { lval.str = string(lex.data[lex.ts:lex.te]); tok = TPLUS; fbreak;};
            '.' => { lval.str = string(lex.data[lex.ts:lex.te]); tok = TDOT; fbreak;};

            operator => {
                lval.str = string(lex.data[lex.ts:lex.te]); tok = int(OP);    
                fbreak;
            };
        *|;

        write exec;
    }%%

    return int(tok);
}