package nfs

import (
	"fmt"
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
	//   - Cookie values are opaque to go-nfs but must be less than 2^63
	//     (bit 63 is reserved internally for synthetic NFS cookies)
	//   - Returning nextCookie=0 signals end of directory (EOF)
	//
	// When cookie is 0, go-nfs will prepend "." and ".." entries automatically.
	// Implementations should NOT include "." and ".." in their returned entries.
	//
	// Error handling:
	//   - Return &NFSStatusError{NFSStatusBadCookie, err} if cookie is invalid/stale
	//   - Return &NFSStatusError{NFSStatusIO, err} for backend errors
	//   - Return &NFSStatusError{NFSStatusNoEnt, err} if directory doesn't exist
	//
	// Ordering note: go-nfs calls ReadDirStream before validating the cookie
	// verifier (because the verifier comes from this method's return value).
	// If the verifier check then fails, the response is discarded. This means
	// any state consumed during the call (e.g., continuation tokens) is lost.
	// Implementations should make state consumption recoverable — for example,
	// by deferring deletion of continuation tokens until after the page is
	// successfully served, or by allowing tokens to be re-fetched.
	ReadDirStream(
		fs billy.Filesystem,
		path []string,
		cookie uint64,
		count int,
	) (entries []os.FileInfo, verifier uint64, nextCookie uint64, err error)
}

// asReadDirStreamer checks whether h (or any handler it wraps) implements
// ReadDirStreamer. This allows wrapper handlers like helpers.CachingHandler
// to transparently forward the streaming interface from the inner handler.
//
// Wrapping handlers should implement Unwrap() Handler to participate.
func asReadDirStreamer(h Handler) (ReadDirStreamer, bool) {
	type unwrapper interface {
		Unwrap() Handler
	}
	for h != nil {
		if s, ok := h.(ReadDirStreamer); ok {
			return s, true
		}
		u, ok := h.(unwrapper)
		if !ok {
			return nil, false
		}
		h = u.Unwrap()
	}
	return nil, false
}

// streamPageResult holds the validated result of fetching a streaming page.
type streamPageResult struct {
	entries       []os.FileInfo
	handlerCookie uint64
	verifier      uint64
	nextCookie    uint64
	eof           bool
}

// fetchStreamPage handles the shared logic of calling ReadDirStream and
// validating cookies/verifiers. Used by both READDIR and READDIRPLUS
// streaming paths to avoid duplicating protocol-level checks.
func fetchStreamPage(
	streamer ReadDirStreamer,
	fs billy.Filesystem,
	p []string,
	cookie, cookieVerif uint64,
	count int,
) (*streamPageResult, error) {
	// Reject synthetic cookies — these are interior/EOF entries that
	// are not valid resumption points. See syntheticCookieBit.
	if cookie&syntheticCookieBit != 0 {
		return nil, &NFSStatusError{NFSStatusBadCookie, nil}
	}

	// Convert client cookie to handler cookie by removing the offset
	// for the synthetic "." and ".." entries that go-nfs prepends.
	// Cookie 0 = first page, cookie 1 = ".." (resume = first real page).
	handlerCookie := uint64(0)
	if cookie > 1 {
		handlerCookie = cookie - streamCookieOffset
	}

	// NOTE: ReadDirStream is called before cookie verifier validation because
	// the verifier value comes from the handler's return. This means the call
	// may consume handler state (e.g., continuation tokens) only for the
	// result to be discarded if the verifier check fails. Implementations
	// should tolerate this — see ReadDirStreamer docs for guidance.
	entries, verifier, nextCookie, err := streamer.ReadDirStream(fs, p, handlerCookie, count)
	if err != nil {
		if nfsErr, ok := err.(*NFSStatusError); ok {
			return nil, nfsErr
		}
		return nil, &NFSStatusError{NFSStatusIO, err}
	}

	// Validate that the handler's nextCookie won't collide with the
	// synthetic cookie space. Handler cookies must be < 2^63 so that
	// adding streamCookieOffset doesn't set the syntheticCookieBit.
	if nextCookie != 0 && nextCookie >= syntheticCookieBit-streamCookieOffset {
		return nil, &NFSStatusError{NFSStatusServerFault, fmt.Errorf(
			"ReadDirStream returned nextCookie %d which is too large (must be < %d)",
			nextCookie, syntheticCookieBit-streamCookieOffset)}
	}

	// If the handler returned verifier=0, compute one from directory mtime.
	if verifier == 0 {
		dirInfo, sErr := fs.Stat(fs.Join(p...))
		if sErr == nil {
			verifier = hashPathAndMtime(fs.Join(p...), dirInfo.ModTime().UnixNano())
		}
	}

	// Validate cookie verifier to detect directory changes between pages.
	// Per RFC 1813 §3.3.16, the client echoes back the verifier from the
	// previous response. If the directory has changed, the verifier won't
	// match and the client must restart the listing from cookie=0.
	if cookie > 0 && cookieVerif > 0 && verifier != cookieVerif {
		return nil, &NFSStatusError{NFSStatusBadCookie, nil}
	}

	return &streamPageResult{
		entries:       entries,
		handlerCookie: handlerCookie,
		verifier:      verifier,
		nextCookie:    nextCookie,
		eof:           nextCookie == 0,
	}, nil
}

// streamingCookie computes the NFS wire cookie for entry i in a streaming page.
// Only the last entry on a non-EOF page gets a real resumption cookie;
// all others get synthetic cookies (syntheticCookieBit set) that go-nfs
// rejects if a client tries to resume from them.
func streamingCookie(eof bool, i, total int, handlerCookie, nextCookie uint64) uint64 {
	if !eof && i == total-1 {
		return nextCookie + streamCookieOffset
	}
	return syntheticCookieBit | (handlerCookie + streamCookieOffset + uint64(i))
}
