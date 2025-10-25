package postgresql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseBinaryVersion(t *testing.T) {
	tests := []struct {
		in  string
		maj int
		min int
		err error
	}{
		{
			in:  "postgres (PostgreSQL) 9.5.7",
			maj: 9,
			min: 5,
		},
		{
			in:  "postgres (PostgreSQL) 9.6.7\n",
			maj: 9,
			min: 6,
		},
		{
			in:  "postgres (PostgreSQL) 10beta1",
			maj: 10,
			min: 0,
		},
		{
			in:  "postgres (PostgreSQL) 10.1.2",
			maj: 10,
			min: 1,
		},
	}

	for i, tt := range tests {
		version, err := parseBinaryVersion(tt.in)
		t.Logf("test #%d", i)
		if tt.err != nil {
			if err == nil {
				t.Fatalf("got no error, wanted error: %v", tt.err)
			} else if tt.err.Error() != err.Error() {
				t.Fatalf("got error: %v, wanted error: %v", err, tt.err)
			}
		} else {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if int(version.Major()) != tt.maj || int(version.Minor()) != tt.min {
				t.Fatalf("#%d: wrong maj.min version: got: %s, want: %d.%d", i, version, tt.maj, tt.min)
			}
		}
	}
}

func TestParseVersion(t *testing.T) {
	tests := []struct {
		in  string
		maj uint64
		min uint64
		err error
	}{
		{
			in:  "9.5.7",
			maj: 9,
			min: 5,
		},
	}

	for i, tt := range tests {
		version, err := ParseVersion(tt.in)
		t.Logf("test #%d", i)
		if tt.err != nil {
			if err == nil {
				t.Fatalf("got no error, wanted error: %v", tt.err)
			} else if tt.err.Error() != err.Error() {
				t.Fatalf("got error: %v, wanted error: %v", err, tt.err)
			}
		} else {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if version.Major() != tt.maj || version.Minor() != tt.min {
				t.Fatalf("#%d: wrong maj.min version : got: %s, want: %d.%d", i, version, tt.maj, tt.min)
			}
		}
	}
}

func TestVersionCompare(t *testing.T) {
	assert.True(t, V96.GreaterThanEqual(V96))
	assert.False(t, V96.LessThan(V96))
}
