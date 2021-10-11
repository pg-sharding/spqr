package qlog

type Qlog interface {
	DumpQuery(q string) error
	Recover(dataFolder string) ([]string, error)
}
