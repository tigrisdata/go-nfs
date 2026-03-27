//go:build integration && (darwin || linux)

package nfs_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	osfs "github.com/go-git/go-billy/v5/osfs"
	nfs "github.com/tigrisdata/go-nfs"
	"github.com/tigrisdata/go-nfs/helpers"
	"golang.org/x/sys/unix"
)

// changeOSFS wraps billy osfs to add the Change interface.
// Replicated from example/osnfs/changeos.go since that's a main package.
type changeOSFS struct {
	billy.Filesystem
}

func newChangeOSFS(fs billy.Filesystem) billy.Filesystem {
	return changeOSFS{fs}
}

func (fs changeOSFS) Chmod(name string, mode os.FileMode) error {
	return os.Chmod(filepath.Join(fs.Root(), name), mode)
}

func (fs changeOSFS) Lchown(name string, uid, gid int) error {
	return os.Lchown(filepath.Join(fs.Root(), name), uid, gid)
}

func (fs changeOSFS) Chown(name string, uid, gid int) error {
	return os.Chown(filepath.Join(fs.Root(), name), uid, gid)
}

func (fs changeOSFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return os.Chtimes(filepath.Join(fs.Root(), name), atime, mtime)
}

func (fs changeOSFS) Mknod(path string, mode uint32, major uint32, minor uint32) error {
	dev := unix.Mkdev(major, minor)
	return unix.Mknod(filepath.Join(fs.Root(), path), mode, int(dev))
}

func (fs changeOSFS) Mkfifo(path string, mode uint32) error {
	return unix.Mkfifo(filepath.Join(fs.Root(), path), mode)
}

func (fs changeOSFS) Link(path string, link string) error {
	return unix.Link(filepath.Join(fs.Root(), path), filepath.Join(fs.Root(), link))
}

func (fs changeOSFS) Socket(path string) error {
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return unix.Bind(fd, &unix.SockaddrUnix{Name: filepath.Join(fs.Root(), path)})
}

// testEnv holds all state for an integration test environment.
type testEnv struct {
	listener  net.Listener
	serverDir string
	mountDir  string
	port      int
	tearOnce  sync.Once
}

// setupTestEnv creates a new NFS server backed by a temp directory,
// mounts it, and returns the test environment.
func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()

	if os.Getuid() != 0 {
		t.Skip("integration tests require root privileges for NFS mount")
	}

	serverDir, err := os.MkdirTemp("", "nfs-server-*")
	if err != nil {
		t.Fatalf("failed to create server dir: %v", err)
	}

	mountDir, err := os.MkdirTemp("", "nfs-mount-*")
	if err != nil {
		os.RemoveAll(serverDir)
		t.Fatalf("failed to create mount dir: %v", err)
	}

	// Create a seed file so the filesystem root is recognized
	seed := filepath.Join(serverDir, ".nfs-test")
	if err := os.WriteFile(seed, []byte(""), 0644); err != nil {
		os.RemoveAll(serverDir)
		os.RemoveAll(mountDir)
		t.Fatalf("failed to create seed file: %v", err)
	}

	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		os.RemoveAll(serverDir)
		os.RemoveAll(mountDir)
		t.Fatalf("failed to listen: %v", err)
	}

	port := listener.Addr().(*net.TCPAddr).Port

	bfs := osfs.New(serverDir)
	bfsPlusChange := newChangeOSFS(bfs)
	handler := helpers.NewNullAuthHandler(bfsPlusChange)
	cacheHelper := helpers.NewCachingHandler(handler, 1024)

	go func() {
		_ = nfs.Serve(listener, cacheHelper)
	}()

	// Give the server a moment to start accepting connections
	time.Sleep(100 * time.Millisecond)

	env := &testEnv{
		listener:  listener,
		serverDir: serverDir,
		mountDir:  mountDir,
		port:      port,
	}

	if err := mountNFS(port, mountDir); err != nil {
		listener.Close()
		os.RemoveAll(serverDir)
		os.RemoveAll(mountDir)
		t.Fatalf("failed to mount NFS: %v", err)
	}

	t.Cleanup(func() { env.teardown(t) })
	return env
}

func (env *testEnv) teardown(t *testing.T) {
	t.Helper()
	env.tearOnce.Do(func() {
		if err := unmountNFS(env.mountDir); err != nil {
			t.Logf("warning: unmount failed: %v", err)
		}
		env.listener.Close()
		os.RemoveAll(env.mountDir)
		os.RemoveAll(env.serverDir)
	})
}

func mountNFS(port int, mountDir string) error {
	// Capture device ID before mount so we can detect when it changes
	var preMountStat unix.Stat_t
	if err := unix.Stat(mountDir, &preMountStat); err != nil {
		return fmt.Errorf("stat mount dir before mount: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.CommandContext(ctx, "mount",
			"-o", fmt.Sprintf("port=%d,mountport=%d,nfsvers=3,tcp,noacl,resvport", port, port),
			"-t", "nfs",
			"localhost:/mount",
			mountDir)
	case "linux":
		cmd = exec.CommandContext(ctx, "mount",
			"-o", fmt.Sprintf("port=%d,mountport=%d,nfsvers=3,noacl,tcp,noac", port, port),
			"-t", "nfs",
			"localhost:/mount",
			mountDir)
	default:
		return fmt.Errorf("unsupported OS: %s", runtime.GOOS)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount failed: %v, output: %s", err, output)
	}

	// Verify mount is usable by checking device ID changed
	if err := waitForMount(mountDir, &preMountStat, 10*time.Second); err != nil {
		return fmt.Errorf("mount not ready: %v", err)
	}
	return nil
}

func waitForMount(mountDir string, preMountStat *unix.Stat_t, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var st unix.Stat_t
		if err := unix.Stat(mountDir, &st); err == nil {
			if st.Dev != preMountStat.Dev {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("mount at %s not ready after %v", mountDir, timeout)
}

func unmountNFS(mountDir string) error {
	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, "umount", mountDir)
		output, err := cmd.CombinedOutput()
		cancel()
		if err == nil {
			return nil
		}
		if attempt == 2 {
			if runtime.GOOS == "darwin" {
				ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel2()
				cmd2 := exec.CommandContext(ctx2, "diskutil", "unmount", "force", mountDir)
				output2, err2 := cmd2.CombinedOutput()
				if err2 != nil {
					return fmt.Errorf("force unmount failed: %v, output: %s (previous: %v, %s)", err2, output2, err, output)
				}
				return nil
			}
			return fmt.Errorf("unmount failed after retries: %v, output: %s", err, output)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

// run executes a command in the given directory, fails the test on error,
// and returns the combined stdout/stderr output.
func run(t *testing.T, dir string, name string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("command %q %v failed: %v\noutput: %s", name, args, err, output)
	}
	return string(output)
}

// runSh executes a shell command string via bash in the given directory.
func runSh(t *testing.T, dir string, script string) string {
	t.Helper()
	return run(t, dir, "bash", "-c", script)
}

// runExpectFail executes a command and expects it to fail (non-zero exit).
func runExpectFail(t *testing.T, dir string, name string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected command %q %v to fail, but it succeeded\noutput: %s", name, args, output)
	}
}

// runShExpectFail executes a shell command string and expects it to fail.
func runShExpectFail(t *testing.T, dir string, script string) {
	t.Helper()
	runExpectFail(t, dir, "bash", "-c", script)
}

// makeSubdir creates a uniquely named subdirectory under base for test isolation.
func makeSubdir(t *testing.T, base string) string {
	t.Helper()
	dir := filepath.Join(base, t.Name())
	run(t, base, "mkdir", "-p", dir)
	return dir
}
