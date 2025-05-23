package qlog

import (
	"bufio"
	"context"
	"os"
	"strings"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
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
	defer func() {
		if err := file.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close file")
		}
	}()

	_, _ = file.WriteString(q)
	_, _ = file.WriteString("\n")
	return nil
}

// TODO : unit tests
func (dw *LocalQlog) Recover(ctx context.Context, path string) ([]string, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return []string{}, err
	}

	spqrlog.Zero.Info().
		Str("path", path).
		Msg("autoconf file found")

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}(file)

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	var queries []string
	qs := ""
	for scanner.Scan() {
		line := scanner.Text()
		query := strings.TrimSpace(line)
		if len(query) > 2 && query[0] == '-' && query[1] == '-' {
			continue
		}
		if len(query) > 0 {
			qs += " " + query

			if query[len(query)-1] == ';' {
				queries = append(queries, qs)
				qs = ""
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return queries, nil
}
