package query

import "testing"

func TestExtractBasePath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "local path",
			path: "/data/archives/bintrail_id=abc-123/event_date=2026-01-10/event_hour=14/events.parquet",
			want: "/data/archives/bintrail_id=abc-123",
		},
		{
			name: "s3 key",
			path: "prefix/bintrail_id=abc-123/event_date=2026-01-10/event_hour=14/events.parquet",
			want: "prefix/bintrail_id=abc-123",
		},
		{
			name: "no prefix",
			path: "bintrail_id=abc-123/event_date=2026-01-10/events.parquet",
			want: "bintrail_id=abc-123",
		},
		{
			name: "no trailing slash",
			path: "bintrail_id=abc-123",
			want: "bintrail_id=abc-123",
		},
		{
			name: "no bintrail_id marker",
			path: "/data/archives/event_date=2026-01-10/events.parquet",
			want: "",
		},
		{
			name: "empty path",
			path: "",
			want: "",
		},
		{
			name: "deep nesting",
			path: "a/b/c/bintrail_id=97adaf56-fe9e-4c1b-9794-b042f7faf197/event_date=2026-03-05/event_hour=18/events.parquet",
			want: "a/b/c/bintrail_id=97adaf56-fe9e-4c1b-9794-b042f7faf197",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractBasePath(tt.path)
			if got != tt.want {
				t.Errorf("extractBasePath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}
