package spqrparser

import (
	"strings"
	"testing"
	"unicode/utf8"
)

const maxHelpLineLength = 100

func TestHelpFileMaxLineLength(t *testing.T) {
	entries, err := helpFS.ReadDir("help")
	if err != nil {
		t.Fatalf("failed to read embedded help directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".txt") {
			continue
		}

		content, err := helpFS.ReadFile("help/" + entry.Name())
		if err != nil {
			t.Fatalf("failed to read help file %s: %v", entry.Name(), err)
		}

		lines := strings.Split(string(content), "\n")
		for lineNum, line := range lines {
			lineLen := utf8.RuneCountInString(line)
			if lineLen > maxHelpLineLength {
				t.Errorf("%s:%d: line length %d exceeds max %d: %s",
					entry.Name(), lineNum+1, lineLen, maxHelpLineLength, line)
			}
		}
	}
}
