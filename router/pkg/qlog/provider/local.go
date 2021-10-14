package qlog

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"

	"github.com/wal-g/tracelog"
)

type LocalQlog struct {
	dataFolder string
}

func NewLocalQlog(dataFolder string) (*LocalQlog, error) {
	return &LocalQlog{dataFolder}, nil
}

func (dw *LocalQlog) DumpQuery(q string) error {
	walPath := filepath.Join(dw.dataFolder, "qlog")
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	file.WriteString(q)
	file.WriteString("\n")
	return nil
}

func (dw *LocalQlog) Recover(dataFolder string) ([]string, error) {
	walPath := filepath.Join(dataFolder, "qlog")
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		tracelog.InfoLogger.Printf("%s log does not exist", walPath)
		return []string{}, nil
	}

	tracelog.InfoLogger.Printf("%s found", walPath)
	file, err := os.Open(walPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

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
