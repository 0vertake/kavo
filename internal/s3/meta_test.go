package s3

import (
	"net/http"
	"testing"
)

// storedMeta is tested from inside the package because the thing to pin is the
// exact key it stores, and no SDK will show that: the aws-sdk-go-v2 lowercases the
// metadata keys of a response before handing them over, so it answers the same
// whether kavo replays "x-amz-meta-colour" or "X-Amz-Meta-Colour". Ceph's suite
// uses botocore, which does not, and that is the difference it caught — a client
// looking up "colour" found "Colour" and concluded the object had no metadata.
func TestStoredMetadata(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]string
		want map[string]string
	}{
		{
			name: "user metadata keeps its name lowercased",
			in:   map[string]string{"x-amz-meta-Colour": "octarine"},
			want: map[string]string{"x-amz-meta-colour": "octarine"},
		},
		{
			name: "the standard headers keep their canonical form",
			in:   map[string]string{"Cache-Control": "no-store", "Content-Language": "en-GB"},
			want: map[string]string{"Cache-Control": "no-store", "Content-Language": "en-GB"},
		},
		{
			name: "aws-chunked describes the transfer and is not kept",
			in:   map[string]string{"Content-Encoding": "gzip, aws-chunked"},
			want: map[string]string{"Content-Encoding": "gzip"},
		},
		{
			name: "an encoding that was only aws-chunked is not stored at all",
			in:   map[string]string{"Content-Encoding": "aws-chunked"},
			want: map[string]string{},
		},
		{
			name: "headers about this exchange are not the object's",
			in:   map[string]string{"Content-Length": "5", "Content-MD5": "deadbeef", "Host": "kavo"},
			want: map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := http.Header{}
			for name, value := range tt.in {
				header.Set(name, value)
			}
			got, err := storedMeta(header)
			if err != nil {
				t.Fatalf("storedMeta: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("stored %v, want %v", got, tt.want)
			}
			for name, value := range tt.want {
				if got[name] != value {
					t.Errorf("stored[%q] = %q, want %q (whole map %v)", name, got[name], value, got)
				}
			}
		})
	}
}
