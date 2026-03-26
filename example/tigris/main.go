// Copyright 2024 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Example implementation of ReadDirStreamer for S3-backed filesystem.
// This file demonstrates how to implement the streaming interface
// for a filesystem backed by object storage (like Tigris or AWS S3).
//
// This example requires the AWS SDK v2 dependency which is not part
// of the go-nfs module. To use, copy to your own project and add:
//   go get github.com/aws/aws-sdk-go-v2
//   go get github.com/aws/aws-sdk-go-v2/service/s3

//go:build ignore

package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	billy "github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
)

// TigrisHandler implements nfs.ReadDirStreamer for S3-backed storage.
type TigrisHandler struct {
	nfshelper.NullAuthHandler
	s3Client *s3.Client
	bucket   string

	// Cookie management for streaming
	streams    sync.Map
	nextCookie atomic.Uint64
}

// streamState holds the continuation token for a paginated listing.
type streamState struct {
	path         string
	continuation *string
	createdAt    time.Time
}

// Verify interface compliance at compile time.
var _ nfs.ReadDirStreamer = (*TigrisHandler)(nil)

// ReadDirStream implements nfs.ReadDirStreamer.
// Called for each page of a directory listing.
func (h *TigrisHandler) ReadDirStream(
	fs billy.Filesystem,
	path []string,
	cookie uint64,
	count int,
) (entries []os.FileInfo, verifier uint64, nextCookie uint64, err error) {
	pathStr := "/" + strings.Join(path, "/")
	prefix := pathToS3Prefix(pathStr)

	var continuation *string

	if cookie > 0 {
		stateVal, ok := h.streams.Load(cookie)
		if !ok {
			return nil, 0, 0, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusBadCookie}
		}
		state := stateVal.(*streamState)
		if state.path != pathStr {
			return nil, 0, 0, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusBadCookie}
		}
		continuation = state.continuation
		h.streams.Delete(cookie)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := h.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(h.bucket),
		Prefix:            aws.String(prefix),
		Delimiter:         aws.String("/"),
		MaxKeys:           aws.Int32(int32(count)),
		ContinuationToken: continuation,
	})
	if err != nil {
		return nil, 0, 0, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusIO, WrappedErr: err}
	}

	entries = make([]os.FileInfo, 0, len(result.CommonPrefixes)+len(result.Contents))

	for _, p := range result.CommonPrefixes {
		name := filepath.Base(strings.TrimSuffix(aws.ToString(p.Prefix), "/"))
		entries = append(entries, &s3FileInfo{
			name:    name,
			size:    0,
			mode:    0755 | os.ModeDir,
			modTime: time.Now(),
			isDir:   true,
			inode:   hashToInode(aws.ToString(p.Prefix)),
		})
	}

	for _, obj := range result.Contents {
		key := aws.ToString(obj.Key)
		if key == prefix {
			continue
		}
		entries = append(entries, &s3FileInfo{
			name:    filepath.Base(key),
			size:    aws.ToInt64(obj.Size),
			mode:    0644,
			modTime: aws.ToTime(obj.LastModified),
			isDir:   false,
			inode:   hashToInode(key),
		})
	}

	// Verifier 0 = disabled (appropriate for S3 virtual directories).
	verifier = 0

	if aws.ToBool(result.IsTruncated) && result.NextContinuationToken != nil {
		newCookie := h.nextCookie.Add(1)
		h.streams.Store(newCookie, &streamState{
			path:         pathStr,
			continuation: result.NextContinuationToken,
			createdAt:    time.Now(),
		})
		nextCookie = newCookie

		go h.cleanupOldStreams()
	} else {
		nextCookie = 0
	}

	return entries, verifier, nextCookie, nil
}

func (h *TigrisHandler) cleanupOldStreams() {
	cutoff := time.Now().Add(-5 * time.Minute)
	h.streams.Range(func(key, value any) bool {
		state := value.(*streamState)
		if state.createdAt.Before(cutoff) {
			h.streams.Delete(key)
		}
		return true
	})
}

// s3FileInfo implements os.FileInfo with proper Sys() for NFS.
type s3FileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	inode   uint64
}

func (fi *s3FileInfo) Name() string       { return fi.name }
func (fi *s3FileInfo) Size() int64        { return fi.size }
func (fi *s3FileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *s3FileInfo) ModTime() time.Time { return fi.modTime }
func (fi *s3FileInfo) IsDir() bool        { return fi.isDir }

func (fi *s3FileInfo) Sys() interface{} {
	mode := uint32(fi.mode.Perm())
	if fi.isDir {
		mode |= syscall.S_IFDIR
	} else {
		mode |= syscall.S_IFREG
	}

	return &syscall.Stat_t{
		Ino:     fi.inode,
		Mode:    mode,
		Nlink:   1,
		Uid:     uint32(os.Getuid()),
		Gid:     uint32(os.Getgid()),
		Size:    fi.size,
		Blksize: 4096,
		Blocks:  (fi.size + 511) / 512,
		Atim:    syscall.Timespec{Sec: fi.modTime.Unix()},
		Mtim:    syscall.Timespec{Sec: fi.modTime.Unix()},
		Ctim:    syscall.Timespec{Sec: fi.modTime.Unix()},
	}
}

func pathToS3Prefix(path string) string {
	path = strings.TrimPrefix(path, "/")
	if path != "" && !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return path
}

func hashToInode(s string) uint64 {
	var h uint64 = 14695981039346656037 // FNV offset basis
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211 // FNV prime
	}
	return h
}

func main() {
	s3Client := createS3Client()
	bucket := os.Getenv("TIGRIS_BUCKET")
	if bucket == "" {
		bucket = "my-dataset"
	}

	handler := &TigrisHandler{
		NullAuthHandler: nfshelper.NewNullAuthHandler(nil),
		s3Client:        s3Client,
		bucket:          bucket,
	}

	cacheHandler := nfshelper.NewCachingHandler(handler, 1000000)

	listener, err := net.Listen("tcp", ":2049")
	if err != nil {
		panic(err)
	}

	println("NFS server listening on :2049")
	println("Mount with: mount -t nfs -o port=2049,mountport=2049,nfsvers=3,tcp localhost:/ /mnt/tigris")

	if err := nfs.Serve(listener, cacheHandler); err != nil {
		panic(err)
	}
}

func createS3Client() *s3.Client {
	// Replace with real initialization, e.g.:
	//
	//   cfg, _ := config.LoadDefaultConfig(context.Background())
	//   return s3.NewFromConfig(cfg, func(o *s3.Options) {
	//       o.BaseEndpoint = aws.String(os.Getenv("AWS_ENDPOINT_URL_S3"))
	//   })
	panic("createS3Client: not implemented — see comment above")
}
