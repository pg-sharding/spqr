package wal

import (
	"bufio"
	"encoding/base64"
	"os"
	"path/filepath"

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

	file, err := os.Create(walPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoded := base64.StdEncoding.EncodeToString([]byte(q))
	file.WriteString(encoded)
	return nil
}

func (dw *DummyWal) Recover(dataFolder string) ([]string, error) {
	walPath := filepath.Join(dataFolder, "dummylog")
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		tracelog.InfoLogger.Println("dummy log does not exist")
		return []string{}, nil
	}

	file, err := os.Open(walPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	var queries []string
	for scanner.Scan() {
		decoded, err := base64.StdEncoding.DecodeString(scanner.Text())
		if err != nil {
			tracelog.ErrorLogger.Fatal("decode error:", err)
			return nil, err
		}
		queries = append(queries, string(decoded))
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return queries, nil
}
