package main

import (
	"encoding/hex"
	"testing"
)

func TestRandomHexLength(t *testing.T) {
	n := 16
	got, err := randomHex(n)
	if err != nil {
		t.Fatalf("randomHex(%d) returned error: %v", n, err)
	}

	wantLen := n * 2
	if len(got) != wantLen {
		t.Fatalf("expected length %d, got %d (%q)", wantLen, len(got), got)
	}
}

func TestRandomHexIsValidHex(t *testing.T) {
	n := 8
	got, err := randomHex(n)
	if err != nil {
		t.Fatalf("randomHex(%d) returned error: %v", n, err)
	}

	if _, err := hex.DecodeString(got); err != nil {
		t.Fatalf("result is not valid hex string: %q, err: %v", got, err)
	}
}

func TestRandomHexZeroLength(t *testing.T) {
	got, err := randomHex(0)
	if err != nil {
		t.Fatalf("randomHex(0) returned error: %v", err)
	}

	if got != "" {
		t.Fatalf("expected empty string for n=0, got %q", got)
	}
}

func TestRandomHexNotAllEqual(t *testing.T) {
	const (
		n     = 8
		count = 100
	)

	values := make([]string, 0, count)
	for i := 0; i < count; i++ {
		v, err := randomHex(n)
		if err != nil {
			t.Fatalf("randomHex(%d) returned error on iteration %d: %v", n, i, err)
		}
		values = append(values, v)
	}

	allEqual := true
	first := values[0]
	for _, v := range values[1:] {
		if v != first {
			allEqual = false
			break
		}
	}

	if allEqual {
		t.Fatalf("all generated values are equal: %v, looks suspicious", values)
	}
}
