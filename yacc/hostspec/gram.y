%{
package hostspec

type HostSpec struct {
	Address  string
	AZ       string
	Priority int
}
%}

%union {
    str  string
    host HostSpec
    int int
}

%token <str> IDENT COLON LBRACKET RBRACKET ZONE PRIORITY

%type <host> hostspec
%type <str>  zone address
%type<int> opt_priority

%start hostspec

%%

opt_priority:
	    /* nothing */ { $$ = 0 } | PRIORITY IDENT { $$ = parseInt($2) }
	;

hostspec:
    address opt_priority
    { $$ = HostSpec{Address: $1, Priority: $2}; setResult(yylex, $$) }
    | address COLON zone opt_priority
    { $$ = HostSpec{Address: $1, AZ: $3, Priority: $4}; setResult(yylex, $$) }
    | address ZONE zone opt_priority
    { $$ = HostSpec{Address: $1, AZ: $3, Priority: $4}; setResult(yylex, $$) }
    ;

zone:
    IDENT { $$ = $1 }
    ;

address:
    IDENT COLON IDENT      { $$ = $1 + ":" + $3 }
    | LBRACKET IDENT RBRACKET COLON IDENT { $$ = "[" + $2 + "]:" + $5 }
    | IDENT                { $$ = $1 }
    ;

%%
