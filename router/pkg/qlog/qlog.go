package qlog

import "context"

type Qlog interface {
	DumpQuery(ctx context.Context, fname string, q string) error
	Recover(ctx context.Context, path string) ([]string, error)
}
