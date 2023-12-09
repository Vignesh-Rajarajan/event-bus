package manager

import (
	"bytes"
	"testing"
)

func TestGetTillLastMessage(t *testing.T) {
	res := []byte("100\n101\n102")
	want, rest := []byte("100\n101\n"), []byte("102")
	truncated, restAct, _ := getTillLastDelimiter(res)

	if !bytes.Equal(truncated, want) || !bytes.Equal(rest, restAct) {
		t.Errorf("want %v got %v rest %v restAct %v", string(want), string(truncated), string(rest), string(restAct))
	}
}

func TestInvalidMessage(t *testing.T) {
	res := []byte("10000")
	_, _, err := getTillLastDelimiter(res)

	if err == nil {
		t.Fatalf("want error got nil")
	}
}
