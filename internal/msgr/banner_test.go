package msgr

import (
	"bytes"
	"errors"
	"testing"
)

func TestBannerEncodingAndNegotiation(t *testing.T) {
	want := []byte{
		'c', 'e', 'p', 'h', ' ', 'v', '2', '\n', 16, 0,
		1, 0, 0, 0, 0, 0, 0, 0,
		1, 0, 0, 0, 0, 0, 0, 0,
	}
	if got := ClientBanner().Encode(); !bytes.Equal(got, want) {
		t.Fatalf("banner = %x, want %x", got, want)
	}
	peer, err := ReadBanner(bytes.NewReader(want), 64)
	if err != nil {
		t.Fatal(err)
	}
	if negotiated, err := NegotiateBanner(ClientBanner(), peer); err != nil || negotiated != Revision1Features {
		t.Fatalf("negotiated=%#x error=%v", negotiated, err)
	}
}

func TestBannerRejectsMalformedAndUnsupported(t *testing.T) {
	oversized := append([]byte(bannerPrefix), 17, 0)
	if _, err := ReadBanner(bytes.NewReader(oversized), 16); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("oversized error = %v", err)
	}
	if _, err := ReadBanner(bytes.NewReader([]byte("ceph v1\n\x10\x00")), 16); !errors.Is(err, ErrMalformed) {
		t.Fatalf("prefix error = %v", err)
	}
	if _, err := NegotiateBanner(ClientBanner(), Banner{Supported: 0}); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("feature error = %v", err)
	}
}

func TestCephCRC32C(t *testing.T) {
	for _, test := range []struct {
		payload string
		want    uint32
	}{
		{"foo bar baz", 1599983188},
		{"whiz bang boom", 4207245113},
	} {
		if got := cephCRC32C(0, []byte(test.payload)); got != test.want {
			t.Fatalf("crc32c(%q) = %d, want %d", test.payload, got, test.want)
		}
	}
}
