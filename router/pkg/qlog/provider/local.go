package qlog

import (
	"bufio"
	"context"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"os"
	"strings"
)

type LocalQlog struct{}

func NewLocalQlog() *LocalQlog {
	return &LocalQlog{}
}

func (dw *LocalQlog) DumpQuery(ctx context.Context, fname string, q string) error {

	// TODO: use
	//ctxQLog, cf := context.WithTimeout(ctx, time.Second * 5)
	//defer cf()
	//
	//ctxQLog.Deadline()

	file, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return err
	}
	defer file.Close()

	_, _ = file.WriteString(q)
	_, _ = file.WriteString("\n")
	return nil
}

func (dw *LocalQlog) Recover(ctx context.Context, path string) ([]string, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		spqrlog.Logger.Printf(spqrlog.LOG, "%s spqrlog does not exist", path)
		return []string{}, err
	}

	spqrlog.Logger.Printf(spqrlog.LOG, "%s found", path)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}(file)

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	var queries []string
	for scanner.Scan() {
		line := scanner.Text()
		query := strings.TrimSpace(line)
		if len(query) > 0 {
			queries = append(queries, query)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return queries, nil
}
