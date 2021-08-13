
package shgoparser

type Show struct {
	Cmd string
}

// The frollowing constants represent SHOW statements.
const (
	ShowDatabasesStr     = "databases"
	ShowPoolsStr         = "pools"
	ShowUnsupportedStr   = "unsupported"
)


// Statement represents a statement.
type Statement interface {
	iStatement()
}


func (*Show) iStatement()          {}

// Tokenizer is the struct used to generate SQL
// tokens for the parser.
type Tokenizer struct {
	InStream      *strings.Reader

	LastError string
}


func NewStringTokenizer(sql string) *Tokenizer {
	return &Tokenizer{InStream: strings.NewReader(sql)}
}
