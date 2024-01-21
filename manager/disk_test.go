package manager

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestInitaliseOnDisk(t *testing.T) {
	dir := getTempDir(t)
	testCreateFile(t, filepath.Join(dir, "chunk1"))
	testCreateFile(t, filepath.Join(dir, "chunk10"))

	onDisk := testNewOnDisk(t, dir)

	want := uint64(11)
	got := onDisk.lastChunkIdx
	if want != got {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestReadAndWriteOnDisk(t *testing.T) {
	onDisk := testNewOnDisk(t, getTempDir(t))

	want := "one\ntwo\nthree\nfour\nfive\n"

	if err := onDisk.Write([]byte(want)); err != nil {
		t.Fatalf("error while writing %v", err)
	}

	chunks, err := onDisk.ListChunks()
	if err != nil {
		t.Fatalf("error while listing chunks %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("received %d chunks want %d", len(chunks), 1)
	}
	chunk := chunks[0].Name
	var b bytes.Buffer
	if err := onDisk.Read(chunk, 0, 100, &b); err != nil {
		t.Fatalf("error while reading %v", err)
	}
	got := b.String()
	if want != got {
		t.Errorf("got %v want %v", got, want)
	}
}

func testCreateFile(t *testing.T, fileName string) {
	t.Helper()
	if _, err := os.Create(fileName); err != nil {
		t.Fatalf("error while creating file %v", err)
	}
}

func testNewOnDisk(t *testing.T, dir string) *EventBusOnDisk {
	t.Helper()
	onDisk, err := NewEventBusOnDisk(dir)
	if err != nil {
		t.Fatalf("error while creating on disk %v", err)
	}
	return onDisk
}

func getTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(os.TempDir(), "lastChunkIdx")
	if err != nil {
		t.Fatalf("error while creating temp dir %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return dir
}