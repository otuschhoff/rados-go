package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/otuschhoff/rados-go/internal/cephx"
)

func TestSameTicketIdentity(t *testing.T) {
	base := cephx.TicketMetadata{SecretID: 7, Fingerprint: sha256.Sum256([]byte{1, 2, 3})}
	if !sameTicketIdentity(base, cephx.TicketMetadata{SecretID: 7, Fingerprint: sha256.Sum256([]byte{1, 2, 3})}) {
		t.Fatal("identical ticket identities differ")
	}
	if sameTicketIdentity(base, cephx.TicketMetadata{SecretID: 8, Fingerprint: sha256.Sum256([]byte{1, 2, 3})}) {
		t.Fatal("different secret IDs compare equal")
	}
	if sameTicketIdentity(base, cephx.TicketMetadata{SecretID: 7, Fingerprint: sha256.Sum256([]byte{1, 2, 4})}) {
		t.Fatal("different ticket blobs compare equal")
	}
}

func TestWaitForCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if err := waitFor(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("canceled wait took %s", elapsed)
	}
}
