package errcounter

type ErrCounter interface {
	ReportError(errtype string)
	ErrorCounts() map[string]uint64
}
