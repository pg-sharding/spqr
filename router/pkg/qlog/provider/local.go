package qlog

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type LocalQlog struct{}

func NewLocalQlog() *LocalQlog {
	return &LocalQlog{}
}

func (dw *LocalQlog) DumpQuery(ctx context.Context, path string, q string) error {

	// TODO: use
	//ctxQLog, cf := context.WithTimeout(ctx, time.Second * 5)
	//defer cf()
	//
	//ctxQLog.Deadline()

	file, err := os.Open(path)

	if err != nil {
		return err
	}
	defer file.Close()

	_, _ = file.WriteString(q)
	_, _ = file.WriteString("\n")
	return nil
}

func (dw *LocalQlog) Recover(ctx context.Context, path string) (string, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		spqrlog.Logger.Printf(spqrlog.LOG, "%s spqrlog does not exist", path)
		return "", err
	}

	spqrlog.Logger.Printf(spqrlog.LOG, "%s found", path)
	// TODO it just reads the whole file, but in the future it should read one sql query at a time
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed reading file: %w", err)
	}
	defer file.Close()
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
