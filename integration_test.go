//go:build integration && (darwin || linux)

package nfs_test

import (
	"fmt"
	"strings"
	"testing"
)

func TestIntegration(t *testing.T) {
	env := setupTestEnv(t)

	t.Run("CreateAndReadFile", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		runSh(t, dir, `echo -n "hello world" > file.txt`)
		got := runSh(t, dir, `cat file.txt`)
		if got != "hello world" {
			t.Fatalf("expected %q, got %q", "hello world", got)
		}
	})

	t.Run("TouchAndStat", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		run(t, dir, "touch", "file.txt")
		out := run(t, dir, "stat", "file.txt")
		if !strings.Contains(out, "file.txt") {
			t.Fatalf("stat output does not mention file.txt: %s", out)
		}
	})

	t.Run("WriteAndAppend", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		runSh(t, dir, `echo "first" > file.txt`)
		runSh(t, dir, `echo "second" >> file.txt`)
		got := runSh(t, dir, `cat file.txt`)
		if !strings.Contains(got, "first") || !strings.Contains(got, "second") {
			t.Fatalf("expected both lines, got %q", got)
		}
	})

	t.Run("OverwriteFile", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		runSh(t, dir, `echo -n "aaa" > file.txt`)
		runSh(t, dir, `echo -n "bbb" > file.txt`)
		got := runSh(t, dir, `cat file.txt`)
		if got != "bbb" {
			t.Fatalf("expected %q, got %q", "bbb", got)
		}
	})

	t.Run("CopyAndDiff", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		runSh(t, dir, `echo -n "data to copy" > src.txt`)
		run(t, dir, "cp", "src.txt", "dst.txt")
		run(t, dir, "diff", "src.txt", "dst.txt")
	})

	t.Run("MkdirAndList", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		run(t, dir, "mkdir", "subdir")
		run(t, dir, "touch", "subdir/a.txt", "subdir/b.txt")
		out := run(t, dir, "ls", "subdir")
		if !strings.Contains(out, "a.txt") || !strings.Contains(out, "b.txt") {
			t.Fatalf("expected a.txt and b.txt in listing, got %q", out)
		}
	})

	t.Run("NestedDirectories", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		run(t, dir, "mkdir", "-p", "a/b/c")
		runSh(t, dir, `echo -n "deep" > a/b/c/file.txt`)
		got := runSh(t, dir, `cat a/b/c/file.txt`)
		if got != "deep" {
			t.Fatalf("expected %q, got %q", "deep", got)
		}
	})

	t.Run("RemoveEmptyDirectory", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		run(t, dir, "mkdir", "emptydir")
		run(t, dir, "rmdir", "emptydir")
		runShExpectFail(t, dir, `ls emptydir`)
	})

	t.Run("RemoveDirectoryRecursive", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		run(t, dir, "mkdir", "-p", "dir/sub")
		run(t, dir, "touch", "dir/sub/f.txt")
		run(t, dir, "rm", "-rf", "dir")
		runShExpectFail(t, dir, `ls dir`)
	})

	t.Run("DeleteFile", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		run(t, dir, "touch", "file.txt")
		run(t, dir, "rm", "file.txt")
		runShExpectFail(t, dir, `ls file.txt`)
	})

	t.Run("RenameFile", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		runSh(t, dir, `echo -n "content" > old.txt`)
		run(t, dir, "mv", "old.txt", "new.txt")
		got := runSh(t, dir, `cat new.txt`)
		if got != "content" {
			t.Fatalf("expected %q, got %q", "content", got)
		}
		runShExpectFail(t, dir, `ls old.txt`)
	})

	t.Run("RenameCrossDirectory", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		run(t, dir, "mkdir", "dir1", "dir2")
		runSh(t, dir, `echo -n "moving" > dir1/file.txt`)
		run(t, dir, "mv", "dir1/file.txt", "dir2/file.txt")
		got := runSh(t, dir, `cat dir2/file.txt`)
		if got != "moving" {
			t.Fatalf("expected %q, got %q", "moving", got)
		}
		runShExpectFail(t, dir, `ls dir1/file.txt`)
	})

	t.Run("CreateAndReadSymlink", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		runSh(t, dir, `echo -n "target data" > target.txt`)
		run(t, dir, "ln", "-s", "target.txt", "link.txt")
		got := runSh(t, dir, `readlink link.txt`)
		got = strings.TrimSpace(got)
		if got != "target.txt" {
			t.Fatalf("expected readlink %q, got %q", "target.txt", got)
		}
		got = runSh(t, dir, `cat link.txt`)
		if got != "target data" {
			t.Fatalf("expected %q through symlink, got %q", "target data", got)
		}
	})

	t.Run("DanglingSymlink", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		run(t, dir, "ln", "-s", "nonexistent", "dangling.txt")
		got := strings.TrimSpace(runSh(t, dir, `readlink dangling.txt`))
		if got != "nonexistent" {
			t.Fatalf("expected readlink %q, got %q", "nonexistent", got)
		}
		runShExpectFail(t, dir, `cat dangling.txt`)
	})

	t.Run("Chmod", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		run(t, dir, "touch", "file.txt")
		run(t, dir, "chmod", "755", "file.txt")
		out := runSh(t, dir, `stat -c '%a' file.txt 2>/dev/null || stat -f '%Lp' file.txt`)
		out = strings.TrimSpace(out)
		if out != "755" {
			t.Fatalf("expected mode 755, got %q", out)
		}
	})

	t.Run("ChmodDirectory", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		run(t, dir, "mkdir", "mydir")
		run(t, dir, "chmod", "700", "mydir")
		out := runSh(t, dir, `stat -c '%a' mydir 2>/dev/null || stat -f '%Lp' mydir`)
		out = strings.TrimSpace(out)
		if out != "700" {
			t.Fatalf("expected mode 700, got %q", out)
		}
	})

	t.Run("LargeFileDD", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		// Write 10 MB file
		run(t, dir, "dd", "if=/dev/urandom", "of=large.bin", "bs=1048576", "count=10")
		// Verify size
		out := runSh(t, dir, `wc -c < large.bin`)
		out = strings.TrimSpace(out)
		if out != "10485760" {
			t.Fatalf("expected 10485760 bytes, got %s", out)
		}
		// Copy and diff to verify integrity
		run(t, dir, "cp", "large.bin", "large2.bin")
		run(t, dir, "diff", "large.bin", "large2.bin")
	})

	t.Run("ManyFiles", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		// Create 500 files in batches using a single shell command
		runSh(t, dir, `for i in $(seq -w 0 499); do touch "file-$i.txt"; done`)
		// Verify count
		out := runSh(t, dir, `ls -1 | wc -l`)
		out = strings.TrimSpace(out)
		if out != "500" {
			t.Fatalf("expected 500 files, got %s", out)
		}
		// Cross-check with find
		out = runSh(t, dir, `find . -maxdepth 1 -type f | wc -l`)
		out = strings.TrimSpace(out)
		if out != "500" {
			t.Fatalf("expected 500 files from find, got %s", out)
		}
	})

	t.Run("ConcurrentWriters", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		// Launch 10 background writers and wait
		script := `
for i in $(seq 0 9); do
  echo -n "data-$i" > "concurrent-$i.txt" &
done
wait
`
		runSh(t, dir, script)
		// Verify all files exist with correct content
		for i := 0; i < 10; i++ {
			got := runSh(t, dir, fmt.Sprintf(`cat concurrent-%d.txt`, i))
			expected := fmt.Sprintf("data-%d", i)
			if got != expected {
				t.Fatalf("file concurrent-%d.txt: expected %q, got %q", i, expected, got)
			}
		}
	})

	t.Run("ReadNonExistent", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		runShExpectFail(t, dir, `cat nonexistent.txt`)
	})

	t.Run("RmNonExistent", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		runExpectFail(t, dir, "rm", "nonexistent.txt")
	})

	t.Run("MkdirExisting", func(t *testing.T) {
		dir := makeSubdir(t, env.mountDir)
		run(t, dir, "mkdir", "existing")
		runExpectFail(t, dir, "mkdir", "existing")
	})
}
