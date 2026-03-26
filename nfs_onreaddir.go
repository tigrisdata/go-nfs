package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"

	billy "github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type readDirArgs struct {
	Handle      []byte
	Cookie      uint64
	CookieVerif uint64
	Count       uint32
}

type readDirEntity struct {
	FileID uint64
	Name   []byte
	Cookie uint64
	Next   bool
}

func onReadDir(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = opAttrErrorFormatter
	obj := readDirArgs{}
	err := xdr.Read(w.req.Body, &obj)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}

	if obj.Count < 1024 {
		return &NFSStatusError{NFSStatusTooSmall, io.ErrShortBuffer}
	}

	fs, p, err := userHandle.FromHandle(obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	// If handler supports streaming, use the streaming path.
	if streamer, ok := userHandle.(ReadDirStreamer); ok {
		return onReadDirStreaming(ctx, w, userHandle, streamer, fs, p, obj)
	}

	// Original path: load all entries via billy.ReadDir.
	contents, verifier, err := getDirListingWithVerifier(userHandle, obj.Handle, obj.CookieVerif)
	if err != nil {
		return err
	}
	if obj.Cookie > 0 && obj.CookieVerif > 0 && verifier != obj.CookieVerif {
		return &NFSStatusError{NFSStatusBadCookie, nil}
	}

	entities := make([]readDirEntity, 0)
	maxBytes := uint32(100) // conservative overhead measure

	started := obj.Cookie == 0
	if started {
		// add '.' and '..' to entities
		dotdotFileID := uint64(0)
		if len(p) > 0 {
			dda := tryStat(fs, p[0:len(p)-1])
			if dda != nil {
				dotdotFileID = dda.Fileid
			}
		}
		dotFileID := uint64(0)
		da := tryStat(fs, p)
		if da != nil {
			dotFileID = da.Fileid
		}
		entities = append(entities,
			readDirEntity{Name: []byte("."), Cookie: 0, Next: true, FileID: dotFileID},
			readDirEntity{Name: []byte(".."), Cookie: 1, Next: true, FileID: dotdotFileID},
		)
	}

	eof := true
	maxEntities := userHandle.HandleLimit() / 2
	for i, c := range contents {
		// cookie equates to index within contents + 2 (for '.' and '..')
		cookie := uint64(i + 2)
		if started {
			maxBytes += 512 // TODO: better estimation.
			if maxBytes > obj.Count || len(entities) > maxEntities {
				eof = false
				break
			}

			attrs := ToFileAttribute(c, path.Join(append(p, c.Name())...))
			entities = append(entities, readDirEntity{
				FileID: attrs.Fileid,
				Name:   []byte(c.Name()),
				Cookie: cookie,
				Next:   true,
			})
		} else if cookie == obj.Cookie {
			started = true
		}
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, p)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := xdr.Write(writer, verifier); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := xdr.Write(writer, len(entities) > 0); err != nil { // next
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if len(entities) > 0 {
		entities[len(entities)-1].Next = false
		// no next for last entity

		for _, e := range entities {
			if err := xdr.Write(writer, e); err != nil {
				return &NFSStatusError{NFSStatusServerFault, err}
			}
		}
	}
	if err := xdr.Write(writer, eof); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	// TODO: track writer size at this point to validate maxcount estimation and stop early if needed.

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}

// streamCookieOffset is added to handler cookies on the wire to avoid
// collisions with the hardcoded "." (cookie=0) and ".." (cookie=1) entries.
// Handler cookies are opaque uint64 values; without this offset, a handler
// returning nextCookie=1 would collide with "..".
const streamCookieOffset = 2

// syntheticCookieBit marks cookies that are NOT valid resumption points.
// Interior entries on non-EOF pages and all entries on EOF pages get this
// bit set. Only the last entry on a non-EOF page has a "real" cookie
// (nextCookie + streamCookieOffset) that clients can use to resume.
// This prevents collisions between synthetic cookies across pages.
const syntheticCookieBit = uint64(1) << 63

// onReadDirStreaming handles READDIR using the ReadDirStreamer interface.
// Memory usage is bounded by the page size rather than directory size.
func onReadDirStreaming(
	ctx context.Context,
	w *response,
	userHandle Handler,
	streamer ReadDirStreamer,
	fs billy.Filesystem,
	p []string,
	obj readDirArgs,
) error {
	// Estimate how many entries to request based on Count.
	count := int(obj.Count / 64) // ~64 bytes per wire entry estimate
	if count < 10 {
		count = 10
	}
	maxEntities := userHandle.HandleLimit() / 2
	if maxEntities > 0 && count > maxEntities {
		count = maxEntities
	}

	// Reject synthetic cookies — these are interior/EOF entries that
	// are not valid resumption points. See syntheticCookieBit.
	if obj.Cookie&syntheticCookieBit != 0 {
		return &NFSStatusError{NFSStatusBadCookie, nil}
	}

	// Convert client cookie to handler cookie by removing the offset
	// for the synthetic "." and ".." entries that go-nfs prepends.
	// Cookie 0 = first page, cookie 1 = ".." (resume = first real page).
	handlerCookie := uint64(0)
	if obj.Cookie > 1 {
		handlerCookie = obj.Cookie - streamCookieOffset
	}

	// NOTE: ReadDirStream is called before cookie verifier validation because
	// the verifier value comes from the handler's return. This means the call
	// may consume handler state (e.g., continuation tokens) only for the
	// result to be discarded if the verifier check fails. Implementations
	// should tolerate this — see ReadDirStreamer docs for guidance.
	entries, verifier, nextCookie, err := streamer.ReadDirStream(fs, p, handlerCookie, count)
	if err != nil {
		if nfsErr, ok := err.(*NFSStatusError); ok {
			return nfsErr
		}
		return &NFSStatusError{NFSStatusIO, err}
	}

	// Validate that the handler's nextCookie won't collide with the
	// synthetic cookie space. Handler cookies must be < 2^63 so that
	// adding streamCookieOffset doesn't set the syntheticCookieBit.
	if nextCookie != 0 && nextCookie+streamCookieOffset >= syntheticCookieBit {
		return &NFSStatusError{NFSStatusServerFault, fmt.Errorf(
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
	if obj.Cookie > 0 && obj.CookieVerif > 0 && verifier != obj.CookieVerif {
		return &NFSStatusError{NFSStatusBadCookie, nil}
	}

	entities := make([]readDirEntity, 0, len(entries)+2)

	// On first request (cookie=0), prepend "." and ".." entries.
	if obj.Cookie == 0 {
		dotdotFileID := uint64(0)
		if len(p) > 0 {
			dda := tryStat(fs, p[0:len(p)-1])
			if dda != nil {
				dotdotFileID = dda.Fileid
			}
		}
		dotFileID := uint64(0)
		da := tryStat(fs, p)
		if da != nil {
			dotFileID = da.Fileid
		}
		entities = append(entities,
			readDirEntity{Name: []byte("."), Cookie: 0, Next: true, FileID: dotFileID},
			readDirEntity{Name: []byte(".."), Cookie: 1, Next: true, FileID: dotdotFileID},
		)
	}

	eof := nextCookie == 0

	for i, entry := range entries {
		attrs := ToFileAttribute(entry, path.Join(append(p, entry.Name())...))

		// Each entry must have a unique cookie per RFC 1813. The last
		// entry on a non-EOF page gets a real resumption cookie. All
		// other entries get synthetic cookies with syntheticCookieBit
		// set, which go-nfs rejects early if a client tries to resume
		// from them (see the check at the top of this function).
		//
		// Synthetic cookies use (streamCookieOffset + index) to ensure
		// uniqueness within a page, plus the page's handlerCookie to
		// ensure uniqueness across pages.
		var cookie uint64
		if eof {
			cookie = syntheticCookieBit | (handlerCookie + streamCookieOffset + uint64(i))
		} else if i == len(entries)-1 {
			cookie = nextCookie + streamCookieOffset
		} else {
			cookie = syntheticCookieBit | (handlerCookie + streamCookieOffset + uint64(i))
		}

		entities = append(entities, readDirEntity{
			FileID: attrs.Fileid,
			Name:   []byte(entry.Name()),
			Cookie: cookie,
			Next:   true,
		})
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, p)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := xdr.Write(writer, verifier); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := xdr.Write(writer, len(entities) > 0); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if len(entities) > 0 {
		entities[len(entities)-1].Next = false
		for _, e := range entities {
			if err := xdr.Write(writer, e); err != nil {
				return &NFSStatusError{NFSStatusServerFault, err}
			}
		}
	}
	if err := xdr.Write(writer, eof); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}

func getDirListingWithVerifier(userHandle Handler, fsHandle []byte, verifier uint64) ([]fs.FileInfo, uint64, error) {
	// figure out what directory it is.
	fs, p, err := userHandle.FromHandle(fsHandle)
	if err != nil {
		return nil, 0, &NFSStatusError{NFSStatusStale, err}
	}

	path := fs.Join(p...)
	// see if the verifier has this dir cached:
	if vh, ok := userHandle.(CachingHandler); verifier != 0 && ok {
		entries := vh.DataForVerifier(path, verifier)
		if entries != nil {
			return entries, verifier, nil
		}
	}
	// load the entries.
	contents, err := fs.ReadDir(path)
	if err != nil {
		if os.IsPermission(err) {
			return nil, 0, &NFSStatusError{NFSStatusAccess, err}
		}
		return nil, 0, &NFSStatusError{NFSStatusNotDir, err}
	}

	sort.Slice(contents, func(i, j int) bool {
		return contents[i].Name() < contents[j].Name()
	})

	if vh, ok := userHandle.(CachingHandler); ok {
		// let the user handler make a verifier if it can.
		v := vh.VerifierFor(path, contents)
		return contents, v, nil
	}

	id := hashPathAndContents(path, contents)
	return contents, id, nil
}

func hashPathAndContents(path string, contents []fs.FileInfo) uint64 {
	//calculate a cookie-verifier.
	vHash := sha256.New()

	// Add the path to avoid collisions of directories with the same content
	vHash.Write([]byte(path))

	for _, c := range contents {
		vHash.Write([]byte(c.Name())) // Never fails according to the docs
	}

	verify := vHash.Sum(nil)[0:8]
	return binary.BigEndian.Uint64(verify)
}

// hashPathAndMtime creates a verifier from a path and mtime.
// Used by the streaming path when the handler returns verifier=0.
func hashPathAndMtime(path string, mtimeNano int64) uint64 {
	h := sha256.New()
	h.Write([]byte(path))
	_ = binary.Write(h, binary.BigEndian, mtimeNano)
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}
