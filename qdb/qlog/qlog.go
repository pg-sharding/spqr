package qlog

import "context"

type Qlog interface {
	DumpQuery(ctx context.Context, q string) error
	Recover(ctx context.Context) ([]string, error)
}
