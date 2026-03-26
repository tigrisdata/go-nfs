package nfs

import (
	"os"

	billy "github.com/go-git/go-billy/v5"
)

// ReadDirStreamer is an optional interface that handlers can implement
// to support paginated directory listing. When implemented, go-nfs will
// use this instead of billy.Filesystem.ReadDir() for READDIR and
// READDIRPLUS operations.
//
// This enables efficient handling of directories with millions of files
// by streaming entries directly from the backend (e.g., S3 ListObjectsV2)
// without buffering the entire listing in memory.
//
// Handlers that do not implement this interface will continue to use
// the existing behavior of calling ReadDir() and paginating in memory.
type ReadDirStreamer interface {
	// ReadDirStream returns a page of directory entries for the given path.
	//
	// Parameters:
	//   - fs: The billy.Filesystem for this mount
	//   - path: Directory path components (e.g., ["dir", "subdir"])
	//   - cookie: Opaque pagination token from previous call (0 for first request)
	//   - count: Suggested maximum entries to return
	//
	// Returns:
	//   - entries: Directory entries for this page (may be fewer than count)
	//   - verifier: Cookie verifier for directory change detection (0 to disable)
	//   - nextCookie: Token for next page (0 indicates this is the last page)
	//   - err: Any error encountered
	//
	// Cookie semantics:
	//   - Cookie 0 always means "start from beginning"
	//   - Cookie values are opaque to go-nfs and can be any uint64
	//   - Returning nextCookie=0 signals end of directory (EOF)
	//
	// When cookie is 0, go-nfs will prepend "." and ".." entries automatically.
	// Implementations should NOT include "." and ".." in their returned entries.
	//
	// Error handling:
	//   - Return &NFSStatusError{NFSStatusBadCookie, err} if cookie is invalid/stale
	//   - Return &NFSStatusError{NFSStatusIO, err} for backend errors
	//   - Return &NFSStatusError{NFSStatusNoEnt, err} if directory doesn't exist
	ReadDirStream(
		fs billy.Filesystem,
		path []string,
		cookie uint64,
		count int,
	) (entries []os.FileInfo, verifier uint64, nextCookie uint64, err error)
}
