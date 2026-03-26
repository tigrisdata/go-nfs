package nfs

import (
	"os"
	"testing"
	"time"

	billy "github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
)

// mockStreamingHandler implements ReadDirStreamer for testing.
type mockStreamingHandler struct {
	pageSize  int
	allFiles  []string
	callCount int
}

func (m *mockStreamingHandler) ReadDirStream(
	fs billy.Filesystem,
	path []string,
	cookie uint64,
	count int,
) ([]os.FileInfo, uint64, uint64, error) {
	m.callCount++

	start := int(cookie)
	if start >= len(m.allFiles) {
		return nil, 0, 0, nil
	}

	end := start + m.pageSize
	if end > len(m.allFiles) {
		end = len(m.allFiles)
	}

	entries := make([]os.FileInfo, 0, end-start)
	for i := start; i < end; i++ {
		entries = append(entries, &mockFileInfo{
			name:    m.allFiles[i],
			size:    1024,
			mode:    0644,
			modTime: time.Now(),
			isDir:   false,
		})
	}

	var nextCookie uint64
	if end < len(m.allFiles) {
		nextCookie = uint64(end)
	}

	return entries, 0, nextCookie, nil
}

type mockFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *mockFileInfo) Name() string       { return fi.name }
func (fi *mockFileInfo) Size() int64        { return fi.size }
func (fi *mockFileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *mockFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *mockFileInfo) IsDir() bool        { return fi.isDir }
func (fi *mockFileInfo) Sys() interface{}   { return nil }

// Tests

func TestReadDirStreamerInterface(t *testing.T) {
	var _ ReadDirStreamer = (*mockStreamingHandler)(nil)
}

func TestStreamingPagination(t *testing.T) {
	files := make([]string, 100)
	for i := range files {
		files[i] = "file_" + string(rune('0'+i/10)) + string(rune('0'+i%10)) + ".txt"
	}

	handler := &mockStreamingHandler{
		pageSize: 10,
		allFiles: files,
	}

	fs := memfs.New()

	// First page (cookie = 0)
	entries, _, nextCookie, err := handler.ReadDirStream(fs, nil, 0, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 10 {
		t.Errorf("expected 10 entries, got %d", len(entries))
	}
	if nextCookie != 10 {
		t.Errorf("expected nextCookie=10, got %d", nextCookie)
	}

	// Middle page (cookie = 50)
	entries, _, nextCookie, err = handler.ReadDirStream(fs, nil, 50, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 10 {
		t.Errorf("expected 10 entries, got %d", len(entries))
	}
	if nextCookie != 60 {
		t.Errorf("expected nextCookie=60, got %d", nextCookie)
	}

	// Last page (cookie = 90)
	entries, _, nextCookie, err = handler.ReadDirStream(fs, nil, 90, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 10 {
		t.Errorf("expected 10 entries, got %d", len(entries))
	}
	if nextCookie != 0 {
		t.Errorf("expected nextCookie=0 (EOF), got %d", nextCookie)
	}

	// Past end (cookie = 100)
	entries, _, nextCookie, err = handler.ReadDirStream(fs, nil, 100, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
	if nextCookie != 0 {
		t.Errorf("expected nextCookie=0, got %d", nextCookie)
	}
}

func TestStreamingCallCount(t *testing.T) {
	files := make([]string, 1000)
	for i := range files {
		files[i] = "file.txt"
	}

	handler := &mockStreamingHandler{
		pageSize: 100,
		allFiles: files,
	}

	fs := memfs.New()

	cookie := uint64(0)
	totalEntries := 0
	for {
		entries, _, nextCookie, err := handler.ReadDirStream(fs, nil, cookie, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		totalEntries += len(entries)
		if nextCookie == 0 {
			break
		}
		cookie = nextCookie
	}

	if totalEntries != 1000 {
		t.Errorf("expected 1000 total entries, got %d", totalEntries)
	}

	if handler.callCount != 10 {
		t.Errorf("expected 10 calls, got %d", handler.callCount)
	}
}

func TestMemoryBoundedness(t *testing.T) {
	handler := &mockStreamingHandler{
		pageSize: 1000,
		allFiles: make([]string, 1_000_000),
	}

	for i := range handler.allFiles {
		handler.allFiles[i] = "file"
	}

	fs := memfs.New()

	entries, _, _, err := handler.ReadDirStream(fs, nil, 0, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(entries) != 1000 {
		t.Errorf("expected 1000 entries, got %d", len(entries))
	}
}

func BenchmarkStreamingReadDir(b *testing.B) {
	files := make([]string, 10000)
	for i := range files {
		files[i] = "file.txt"
	}

	handler := &mockStreamingHandler{
		pageSize: 1000,
		allFiles: files,
	}

	fs := memfs.New()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cookie := uint64(0)
		for {
			_, _, nextCookie, _ := handler.ReadDirStream(fs, nil, cookie, 1000)
			if nextCookie == 0 {
				break
			}
			cookie = nextCookie
		}
	}
}
