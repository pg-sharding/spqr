package asynctracelog

import "github.com/wal-g/tracelog"

func Printf(f string, a ...interface{}) {
	// very bad
	go func() {
		tracelog.InfoLogger.Printf(f, a)
	}()
}

func PrintError(err error) {
	tracelog.InfoLogger.PrintError(err)
}
