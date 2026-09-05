%{
package hostspec

type HostSpec struct {
	Address string
	AZ      string
}
%}

%union {
    str  string
    host HostSpec
}

%token <str> IDENT COLON LBRACKET RBRACKET ZONE

%type <host> hostspec
%type <str>  zone address

%%

hostspec:
    address
    { $$ = HostSpec{Address: $1}; setResult(yylex, $$) }
    | address COLON zone
    { $$ = HostSpec{Address: $1, AZ: $3}; setResult(yylex, $$) }
    | address ZONE zone
    { $$ = HostSpec{Address: $1, AZ: $3}; setResult(yylex, $$) }
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
