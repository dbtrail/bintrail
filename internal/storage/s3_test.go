package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// mockS3 implements s3API for testing.
type mockS3 struct {
	objects map[string][]byte // key → content

	headBucketErr error
	putErr        error
	getErr        error
	headErr       error
	deleteErr     error
	listErr       error
}

func newMockS3() *mockS3 {
	return &mockS3{objects: make(map[string][]byte)}
}

func (m *mockS3) HeadBucket(_ context.Context, _ *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return &s3.HeadBucketOutput{}, m.headBucketErr
}

func (m *mockS3) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.putErr != nil {
		return nil, m.putErr
	}
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	m.objects[*input.Key] = data
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	data, ok := m.objects[*input.Key]
	if !ok {
		return nil, &types.NoSuchKey{Message: aws.String("not found")}
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (m *mockS3) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if m.headErr != nil {
		return nil, m.headErr
	}
	if _, ok := m.objects[*input.Key]; !ok {
		return nil, &types.NotFound{Message: aws.String("not found")}
	}
	return &s3.HeadObjectOutput{}, nil
}

func (m *mockS3) DeleteObject(_ context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	delete(m.objects, *input.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3) ListObjectsV2(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	prefix := aws.ToString(input.Prefix)
	var contents []types.Object
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			contents = append(contents, types.Object{Key: aws.String(k)})
		}
	}
	return &s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: aws.Bool(false),
	}, nil
}

func newTestBackend(t *testing.T, mock *mockS3, prefix string) *S3Backend {
	t.Helper()
	ctx := context.Background()
	b, err := newS3BackendFromClient(ctx, mock, S3Config{
		Bucket: "test-bucket",
		Prefix: prefix,
	})
	if err != nil {
		t.Fatalf("newS3BackendFromClient: %v", err)
	}
	return b
}

func TestPutAndGet(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	content := "hello world"
	if err := b.Put(ctx, "test.parquet", strings.NewReader(content)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, err := b.Get(ctx, "test.parquet")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != content {
		t.Errorf("Get returned %q, want %q", data, content)
	}
}

func TestPutWithPrefix(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "bintrail/archives")
	ctx := context.Background()

	if err := b.Put(ctx, "2026/data.parquet", strings.NewReader("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The mock should have the full key with prefix.
	if _, ok := mock.objects["bintrail/archives/2026/data.parquet"]; !ok {
		keys := make([]string, 0, len(mock.objects))
		for k := range mock.objects {
			keys = append(keys, k)
		}
		t.Errorf("expected key with prefix, got keys: %v", keys)
	}
}

func TestList(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "pfx")
	ctx := context.Background()

	// Add objects directly to mock with full prefixed keys.
	mock.objects["pfx/a/1.parquet"] = []byte("a1")
	mock.objects["pfx/a/2.parquet"] = []byte("a2")
	mock.objects["pfx/b/3.parquet"] = []byte("b3")

	keys, err := b.List(ctx, "a/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(keys) != 2 {
		t.Fatalf("List returned %d keys, want 2: %v", len(keys), keys)
	}

	// Keys should be relative (prefix stripped).
	for _, k := range keys {
		if !strings.HasPrefix(k, "a/") {
			t.Errorf("key %q does not start with 'a/'", k)
		}
	}
}

func TestListAll(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	mock.objects["x.parquet"] = []byte("x")
	mock.objects["y.parquet"] = []byte("y")

	keys, err := b.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("List returned %d keys, want 2", len(keys))
	}
}

func TestDelete(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	mock.objects["doomed.parquet"] = []byte("bye")

	if err := b.Delete(ctx, "doomed.parquet"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, ok := mock.objects["doomed.parquet"]; ok {
		t.Error("Delete did not remove the object")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	// S3 DeleteObject on a non-existent key is a no-op (not an error).
	if err := b.Delete(ctx, "ghost.parquet"); err != nil {
		t.Fatalf("Delete non-existent: %v", err)
	}
}

func TestDeleteError(t *testing.T) {
	mock := newMockS3()
	mock.deleteErr = fmt.Errorf("delete forbidden")
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	err := b.Delete(ctx, "any.parquet")
	if err == nil || !strings.Contains(err.Error(), "delete forbidden") {
		t.Errorf("expected delete error, got: %v", err)
	}
}

func TestGetError(t *testing.T) {
	mock := newMockS3()
	mock.getErr = fmt.Errorf("access denied")
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	_, err := b.Get(ctx, "any.parquet")
	if err == nil || !strings.Contains(err.Error(), "access denied") {
		t.Errorf("expected get error, got: %v", err)
	}
}

func TestExists(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	mock.objects["present.parquet"] = []byte("here")

	ok, err := b.Exists(ctx, "present.parquet")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !ok {
		t.Error("Exists returned false for existing object")
	}

	ok, err = b.Exists(ctx, "missing.parquet")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if ok {
		t.Error("Exists returned true for missing object")
	}
}

func TestExistsNoSuchKey(t *testing.T) {
	mock := newMockS3()
	// Some S3-compatible backends (Ceph, Wasabi) return NoSuchKey
	// from HeadObject instead of NotFound.
	mock.headErr = &types.NoSuchKey{Message: aws.String("not found")}
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	ok, err := b.Exists(ctx, "anything")
	if err != nil {
		t.Fatalf("Exists with NoSuchKey: %v", err)
	}
	if ok {
		t.Error("Exists returned true for NoSuchKey")
	}
}

func TestExistsHTTP404(t *testing.T) {
	mock := newMockS3()
	// Override HeadObject to return a generic HTTP 404 (some S3-compatible
	// backends use this instead of types.NotFound).
	mock.headErr = &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 404},
		},
		Err: fmt.Errorf("not found"),
	}
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	ok, err := b.Exists(ctx, "anything")
	if err != nil {
		t.Fatalf("Exists with HTTP 404: %v", err)
	}
	if ok {
		t.Error("Exists returned true for HTTP 404")
	}
}

func TestExistsRealError(t *testing.T) {
	mock := newMockS3()
	mock.headErr = fmt.Errorf("network timeout")
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	_, err := b.Exists(ctx, "anything")
	if err == nil {
		t.Fatal("Exists should return error for non-404 failures")
	}
	if !strings.Contains(err.Error(), "network timeout") {
		t.Errorf("error should wrap original: %v", err)
	}
}

func TestNewS3BackendValidation(t *testing.T) {
	ctx := context.Background()

	// Empty bucket — use newS3BackendFromClient directly to avoid loading
	// real AWS config from the environment.
	_, err := newS3BackendFromClient(ctx, newMockS3(), S3Config{})
	if err == nil || !strings.Contains(err.Error(), "bucket name is required") {
		t.Errorf("expected bucket validation error, got: %v", err)
	}
}

func TestNewS3BackendHeadBucketFailure(t *testing.T) {
	mock := newMockS3()
	mock.headBucketErr = fmt.Errorf("access denied")
	ctx := context.Background()

	_, err := newS3BackendFromClient(ctx, mock, S3Config{Bucket: "locked-bucket"})
	if err == nil || !strings.Contains(err.Error(), "validate bucket") {
		t.Errorf("expected bucket validation error, got: %v", err)
	}
}

func TestPutError(t *testing.T) {
	mock := newMockS3()
	mock.putErr = fmt.Errorf("write failed")
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	err := b.Put(ctx, "fail.parquet", strings.NewReader("data"))
	if err == nil || !strings.Contains(err.Error(), "write failed") {
		t.Errorf("expected put error, got: %v", err)
	}
}

func TestGetMissing(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	_, err := b.Get(ctx, "missing.parquet")
	if err == nil {
		t.Fatal("Get missing key should return error")
	}
}

func TestListError(t *testing.T) {
	mock := newMockS3()
	mock.listErr = fmt.Errorf("permission denied")
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	_, err := b.List(ctx, "")
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("expected list error, got: %v", err)
	}
}

func TestKeyValidation(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	cases := []struct {
		key     string
		wantMsg string
	}{
		{"", "must not be empty"},
		{"/bad", "must not start with /"},
	}

	for _, tc := range cases {
		type op struct {
			name string
			fn   func() error
		}
		ops := []op{
			{"Put", func() error { return b.Put(ctx, tc.key, strings.NewReader("x")) }},
			{"Get", func() error { _, err := b.Get(ctx, tc.key); return err }},
			{"Delete", func() error { return b.Delete(ctx, tc.key) }},
			{"Exists", func() error { _, err := b.Exists(ctx, tc.key); return err }},
		}
		for _, o := range ops {
			err := o.fn()
			if err == nil || !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("%s(%q): want error containing %q, got: %v", o.name, tc.key, tc.wantMsg, err)
			}
		}
	}
}

func TestPrefixNormalization(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"bintrail", "bintrail/"},
		{"bintrail/", "bintrail/"},
		{"a/b/c", "a/b/c/"},
		{"a/b/c/", "a/b/c/"},
	}

	for _, tt := range tests {
		mock := newMockS3()
		b := newTestBackend(t, mock, tt.input)
		if b.prefix != tt.want {
			t.Errorf("prefix %q → %q, want %q", tt.input, b.prefix, tt.want)
		}
	}
}

func TestListPagination(t *testing.T) {
	// Build a mock that returns results in two pages.
	mock := &paginatedMockS3{
		mockS3: mockS3{objects: make(map[string][]byte)},
		pages: [][]string{
			{"a.parquet", "b.parquet"},
			{"c.parquet"},
		},
	}
	ctx := context.Background()
	b, err := newS3BackendFromClient(ctx, mock, S3Config{Bucket: "test"})
	if err != nil {
		t.Fatalf("newS3BackendFromClient: %v", err)
	}

	keys, err := b.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("List returned %d keys, want 3: %v", len(keys), keys)
	}
}

// paginatedMockS3 extends mockS3 with paginated List responses.
type paginatedMockS3 struct {
	mockS3
	pages    [][]string
	listCall int
}

func (m *paginatedMockS3) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.listCall >= len(m.pages) {
		return &s3.ListObjectsV2Output{IsTruncated: aws.Bool(false)}, nil
	}
	page := m.pages[m.listCall]
	m.listCall++

	var contents []types.Object
	for _, k := range page {
		contents = append(contents, types.Object{Key: aws.String(k)})
	}

	hasMore := m.listCall < len(m.pages)
	var nextToken *string
	if hasMore {
		nextToken = aws.String(fmt.Sprintf("token-%d", m.listCall))
	}

	return &s3.ListObjectsV2Output{
		Contents:              contents,
		IsTruncated:           aws.Bool(hasMore),
		NextContinuationToken: nextToken,
	}, nil
}

// Verify S3Backend satisfies the Backend interface at compile time.
var _ Backend = (*S3Backend)(nil)
