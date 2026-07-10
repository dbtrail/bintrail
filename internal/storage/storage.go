// Package storage defines an abstract storage backend interface for managing
// Parquet files and other binary objects. The interface is provider-agnostic,
// currently implemented for S3 and S3-compatible services (MinIO, LocalStack).
// Additional providers (GCS, Azure Blob) can be added as new Backend implementations.
//
// In BYOS (Bring Your Own Storage) mode, customers configure their own storage
// backend credentials locally — dbtrail never receives or logs these credentials.
package storage

import (
	"context"
	"errors"
	"io"
)

// ErrObjectExists is returned by ConditionalPutter.PutIfAbsent when an object
// already exists at the target key. Callers detect it with errors.Is.
var ErrObjectExists = errors.New("storage: object already exists")

// Backend defines the storage abstraction for Parquet file management.
// All keys are relative paths (no leading slash). Implementations handle
// provider-specific details like bucket names and key prefixes internally.
type Backend interface {
	// Put uploads the content from r to the given key. If an object already
	// exists at key, it is overwritten.
	Put(ctx context.Context, key string, r io.Reader) error

	// Get returns a reader for the content at the given key. The caller
	// must close the returned ReadCloser when done. If the key does not
	// exist, Get returns an error.
	Get(ctx context.Context, key string) (io.ReadCloser, error)

	// List returns all keys under the given prefix. The returned keys are
	// relative to the backend's configured prefix — they match the keys
	// passed to Put/Get/Delete. An empty prefix lists all keys.
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete removes the object at the given key. Deleting a non-existent
	// key is not an error.
	Delete(ctx context.Context, key string) error

	// Exists checks whether an object exists at the given key.
	Exists(ctx context.Context, key string) (bool, error)
}

// ConditionalPutter is an optional interface a Backend can implement when the
// underlying service supports atomic create-if-absent writes. Callers assert
// for it and fall back to a non-atomic Exists+Put when it is not implemented.
type ConditionalPutter interface {
	// PutIfAbsent uploads the content from r to the given key only if no
	// object exists there, atomically. If an object is already present it
	// returns an error wrapping ErrObjectExists and writes nothing.
	PutIfAbsent(ctx context.Context, key string, r io.Reader) error
}
