package console

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// writeBaselineFile writes <dir>/<parts...> with the given content and mtime.
func writeBaselineFile(t *testing.T, dir string, content string, mtime time.Time, parts ...string) {
	t.Helper()
	p := filepath.Join(append([]string{dir}, parts...)...)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if !mtime.IsZero() {
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
}

const detailSnapDir = "2026-06-10T12-00-00Z"
const detailSnapAt = "2026-06-10 12:00:00"

func detailQuery(at string) string { return "?at=" + url.QueryEscape(at) }

// newDetailFixture builds a complete two-table snapshot with staggered mtimes.
func newDetailFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	base := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	writeBaselineFile(t, dir, "aaaa", base, detailSnapDir, "shop", "orders.parquet")
	writeBaselineFile(t, dir, "bbbbbbbb", base.Add(90*time.Second), detailSnapDir, "shop", "users.parquet")
	writeBaselineFile(t, dir, "", base.Add(2*time.Minute), detailSnapDir, "_SUCCESS")
	writeBaselineFile(t, dir, `{"version":1}`, base.Add(2*time.Minute), detailSnapDir, "_MANIFEST")
	return dir
}

func TestBaselineFiles_localDetail(t *testing.T) {
	dir := newDetailFixture(t)
	srv := newBaselineServer(t, dir, true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselineFilesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Time != detailSnapAt || got.Incomplete {
		t.Fatalf("header = %+v, want time %s and complete", got, detailSnapAt)
	}
	if len(got.Tables) != 2 ||
		got.Tables[0].Schema != "shop" || got.Tables[0].Table != "orders" || got.Tables[0].SizeBytes != 4 ||
		got.Tables[1].Table != "users" || got.Tables[1].SizeBytes != 8 {
		t.Fatalf("tables = %+v, want orders(4) then users(8)", got.Tables)
	}
	// 4 files total: two tables + two markers; 4+8+0+13 bytes.
	if got.Files != 4 || got.TotalBytes != 25 {
		t.Fatalf("files/bytes = %d/%d, want 4/25", got.Files, got.TotalBytes)
	}
	// Span runs from the first parquet to the markers: 120s.
	if got.WriteSpanSeconds != 120 || got.WroteFrom != "2026-06-10 12:00:00" || got.WroteTo != "2026-06-10 12:02:00" {
		t.Fatalf("span = %v (%s → %s), want 120s", got.WriteSpanSeconds, got.WroteFrom, got.WroteTo)
	}
}

func TestBaselineFiles_refusals(t *testing.T) {
	dir := newDetailFixture(t)
	srv := newBaselineServer(t, dir, true)

	rec, body := doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery("2026-01-01 00:00:00"), "")
	if rec.Code != 404 {
		t.Fatalf("missing snapshot: code = %d, body = %s, want 404", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "GET", "/api/baselines/files?at=lunes", "")
	if rec.Code != 400 {
		t.Fatalf("bad at: code = %d, body = %s, want 400", rec.Code, body)
	}
	// No baseline source configured at all.
	srvNone := newBaselineServer(t, "", false)
	rec, body = doServersReq(t, srvNone, "GET", "/api/baselines/files"+detailQuery(detailSnapAt), "")
	if rec.Code != 404 {
		t.Fatalf("unconfigured: code = %d, body = %s, want 404", rec.Code, body)
	}
}

func TestBaselineDownload_localTarRoundTrip(t *testing.T) {
	dir := newDetailFixture(t)
	srv := newBaselineServer(t, dir, true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/download"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/gzip" {
		t.Fatalf("content-type = %q", ct)
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.Contains(cd, "dbtrail-backup-"+detailSnapDir+".tar.gz") {
		t.Fatalf("content-disposition = %q", cd)
	}
	got := untarAll(t, body)
	want := map[string]string{
		detailSnapDir + "/_MANIFEST":           `{"version":1}`,
		detailSnapDir + "/_SUCCESS":            "",
		detailSnapDir + "/shop/orders.parquet": "aaaa",
		detailSnapDir + "/shop/users.parquet":  "bbbbbbbb",
	}
	if len(got) != len(want) {
		t.Fatalf("entries = %v, want %v", keysOf(got), keysOf(want))
	}
	for name, content := range want {
		if got[name] != content {
			t.Fatalf("entry %s = %q, want %q", name, got[name], content)
		}
	}
}

func TestBaselineDownload_refusesIncomplete(t *testing.T) {
	dir := t.TempDir()
	writeBaselineFile(t, dir, "aaaa", time.Time{}, detailSnapDir, "shop", "orders.parquet")
	writeBaselineFile(t, dir, "", time.Time{}, detailSnapDir, "_INCOMPLETE")
	srv := newBaselineServer(t, dir, true)

	rec, body := doServersReq(t, srv, "GET", "/api/baselines/download"+detailQuery(detailSnapAt), "")
	if rec.Code != 409 {
		t.Fatalf("download: code = %d, body = %s, want 409", rec.Code, body)
	}
	// The detail stays readable and says so.
	rec, body = doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("files: code = %d, body = %s", rec.Code, body)
	}
	var got baselineFilesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Incomplete {
		t.Fatal("files: want incomplete=true for an _INCOMPLETE snapshot")
	}
}

// fakeObjectStore serves an in-memory key space as the S3 seam.
type fakeObjectStore struct {
	objects map[string]string // key → content
	mtime   time.Time
}

func (f *fakeObjectStore) ListInfo(_ context.Context, prefix string) ([]storage.ObjectInfo, error) {
	var out []storage.ObjectInfo
	for k, v := range f.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, storage.ObjectInfo{Key: k, Size: int64(len(v)), LastModified: f.mtime})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (f *fakeObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	v, ok := f.objects[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(strings.NewReader(v)), nil
}

func TestBaselineDownload_s3RoundTrip(t *testing.T) {
	fake := &fakeObjectStore{
		mtime: time.Date(2026, 6, 10, 12, 1, 0, 0, time.UTC),
		objects: map[string]string{
			detailSnapDir + "/shop/orders.parquet": "aaaa",
			detailSnapDir + "/_SUCCESS":            "",
			// A sibling snapshot that must NOT leak into this one's archive.
			"2026-06-01T00-00-00Z/shop/orders.parquet": "old",
		},
	}
	orig := newBaselineObjectStore
	newBaselineObjectStore = func(_ context.Context, src string) (baselineObjectStore, error) {
		if src != "s3://bkt/baselines" {
			t.Fatalf("store opened for %q", src)
		}
		return fake, nil
	}
	t.Cleanup(func() { newBaselineObjectStore = orig })

	srv := newBaselineServer(t, "s3://bkt/baselines", true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/download"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	got := untarAll(t, body)
	if len(got) != 2 || got[detailSnapDir+"/shop/orders.parquet"] != "aaaa" {
		t.Fatalf("entries = %v, want exactly the requested snapshot's two files", keysOf(got))
	}

	// And the detail endpoint over the same fake.
	rec, body = doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("files: code = %d, body = %s", rec.Code, body)
	}
	var det baselineFilesResponse
	if err := json.Unmarshal(body, &det); err != nil {
		t.Fatal(err)
	}
	if len(det.Tables) != 1 || det.Tables[0].SizeBytes != 4 || det.TotalBytes != 4 || det.Files != 2 {
		t.Fatalf("detail = %+v, want one 4-byte table across 2 files", det)
	}
}

func untarAll(t *testing.T, b []byte) map[string]string {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("gzip: %v", err)
	}
	tr := tar.NewReader(gz)
	out := map[string]string{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar: %v", err)
		}
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, tr); err != nil {
			t.Fatalf("tar body: %v", err)
		}
		out[hdr.Name] = buf.String()
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close (truncated archive?): %v", err)
	}
	return out
}

func keysOf(m map[string]string) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
