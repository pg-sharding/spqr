package transferworker

type TaskGroupWorkerState struct {
	/* TODO: additional debug state info */
	Cancel func()
}
