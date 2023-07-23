lexgen:
	ragel -Z -G2 -o lex.go lex.rl

yaccgen:
	goyacc -o gram.go -p yy gram.y

gen: lexgen yaccgen