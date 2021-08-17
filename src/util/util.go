package util

import "github.com/wal-g/tracelog"

func Fatal(err error) {
	tracelog.ErrorLogger.PrintError(err)
}

func Info(err error) {
	//tracelog.InfoLogger.PrintError(err)
}
