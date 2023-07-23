package spqrparser

import (
    "strings"
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
        ident_start	=	[A-Za-z\200-\377_];
        ident_cont	=	[A-Za-z\200-\377_0-9$];

        identifier	=	ident_start ident_cont*;

        qidentifier	=	'"' ident_start ident_cont* '"' ;

        horiz_space	= [ \t\f];
        newline		=	[\n\r];
        non_newline	=	[^\n\r];

        sql_comment = '-''-' non_newline*;
        c_style_comment = '/''*' (any - '*''/')* '*''/';
        comment		= sql_comment | c_style_comment;


        whitespace	=	space+;


        op_chars	=	( '~' | '!' | '@' | '#' | '^' | '&' | '|' | '`' | '?' | '+' | '-' | '*' | '\\' | '%' | '<' | '>' | '=' ) ;
        operator	=	op_chars+;

        integer = digit+;

        sconst = '\'' (any-'\'')* '\'';
        
        main := |*
            whitespace => { /* do nothing */ };
            # integer const is string const 
            comment => {/* nothing */};
            integer =>  { lval.str = string(lex.data[lex.ts:lex.te]); tok = SCONST; fbreak;};

            qidentifier      => { lval.str = string(lex.data[lex.ts + 1:lex.te - 1]); tok = IDENT; fbreak;};
            identifier      => { 
                
                lval.str = strings.ToLower(string(lex.data[lex.ts:lex.te]));
                if ttype, ok := reservedWords[lval.str]; ok {
                    tok = ttype;
                } else {
                    tok = IDENT; 
                }
                fbreak;};
            sconst => { lval.str = string(lex.data[lex.ts + 1:lex.te - 1]); tok = SCONST; fbreak;};

            '=' => { lval.str = string(lex.data[lex.ts:lex.te]); tok = TEQ; fbreak;};

            operator => {
                lval.str = string(lex.data[lex.ts:lex.te]); tok = int(OP);    
                fbreak;
            };
        *|;

        write exec;
    }%%

    return int(tok);
}