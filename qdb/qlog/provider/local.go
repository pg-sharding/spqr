package qlog

import (
	"bufio"
	"context"
	"os"
	"strings"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb/qlog"
)

type LocalQlog struct {
	fname string
}

func NewLocalQlog(fname string) qlog.Qlog {
	return &LocalQlog{
		fname: fname,
	}
}

func (dw *LocalQlog) DumpQuery(ctx context.Context, q string) error {

	// TODO: use
	//ctxQLog, cf := context.WithTimeout(ctx, time.Second * 5)
	//defer cf()
	//
	//ctxQLog.Deadline()

	file, err := os.OpenFile(dw.fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return err
	}
	defer file.Close()

	_, _ = file.WriteString(q)
	_, _ = file.WriteString("\n")
	return nil
}

func (dw *LocalQlog) Recover(ctx context.Context) ([]string, error) {
	if _, err := os.Stat(dw.fname); os.IsNotExist(err) {
		return []string{}, err
	}

	spqrlog.Logger.Printf(spqrlog.LOG, "%s found", dw.fname)
	file, err := os.Open(dw.fname)
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
