package wal

type Wal interface {
	DumpQuery(q string) error
	Recover(dataFolder string) ([]string, error)
}