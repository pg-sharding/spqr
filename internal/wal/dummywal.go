package wal

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"

	"github.com/wal-g/tracelog"
)

type DummyWal struct {
	dataFolder string
}

func NewDummyWal(dataFolder string) (*DummyWal, error) {
	return &DummyWal{dataFolder}, nil
}

func (dw *DummyWal) DumpQuery(q string) error {
	walPath := filepath.Join(dw.dataFolder, "dummylog")
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	file.WriteString(q)
	file.WriteString("\n")
	return nil
}

func (dw *DummyWal) Recover(dataFolder string) ([]string, error) {
	walPath := filepath.Join(dataFolder, "dummylog")
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
