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
}

%token <str> IDENT COLON LBRACKET RBRACKET ZONE PRIORITY

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
    | address PRIORITY IDENT
    { $$ = HostSpec{Address: $1, Priority: parseInt($3)}; setResult(yylex, $$) }
    | address COLON zone PRIORITY IDENT
    { $$ = HostSpec{Address: $1, AZ: $3, Priority: parseInt($5)}; setResult(yylex, $$) }
    | address ZONE zone PRIORITY IDENT
    { $$ = HostSpec{Address: $1, AZ: $3, Priority: parseInt($5)}; setResult(yylex, $$) }
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
