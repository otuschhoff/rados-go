package mon

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
)

func TestBootstrapConfigPrecedence(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "ceph.conf")
	data := []byte("[global]\nmon host = v2:192.0.2.1:3300/0\nkeyring = /etc/$cluster/$name.keyring\nfsid = 00010203-0405-0607-0809-0a0b0c0d0e0f\n[client.test]\nkeyring = /file/client.keyring\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GO_LIBRADOS_MON_HOST", "192.0.2.2:3300")
	t.Setenv("GO_LIBRADOS_KEYRING", "/env/keyring")

	withoutEnvironment, err := LoadBootstrapConfig(path, BootstrapConfig{Entity: "client.test"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if withoutEnvironment.KeyringPath != "/file/client.keyring" || withoutEnvironment.MonitorSeeds[0] != "v2:192.0.2.1:3300/0" {
		t.Fatalf("file config = %+v", withoutEnvironment)
	}

	withOverrides, err := LoadBootstrapConfig(path, BootstrapConfig{Entity: "client.test", MonitorSeeds: []string{"192.0.2.3:3300"}, KeyringPath: "/option/keyring"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if withOverrides.KeyringPath != "/option/keyring" || len(withOverrides.MonitorSeeds) != 1 || withOverrides.MonitorSeeds[0] != "192.0.2.3:3300" || withOverrides.ExpectedFSID == nil {
		t.Fatalf("overridden config = %+v", withOverrides)
	}
}

func TestBootstrapConfigRejectsInvalidFSID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ceph.conf")
	if err := os.WriteFile(path, []byte("[global]\nmon_host=192.0.2.1\nfsid=invalid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadBootstrapConfig(path, BootstrapConfig{}, false); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("error = %v", err)
	}
}

func TestParseConfigBoundsAndComments(t *testing.T) {
	sections, err := ParseConfig([]byte("[global] # selected\nmon   host = v2:192.0.2.1:3300/0 ; preferred\n[client.test]\nkeyring = '/keys/a;b'\n"))
	if err != nil {
		t.Fatal(err)
	}
	if sections["global"]["mon_host"] != "v2:192.0.2.1:3300/0" || sections["client.test"]["keyring"] != "'/keys/a;b'" {
		t.Fatalf("sections = %#v", sections)
	}
	if _, err := ParseConfig([]byte(strings.Repeat("x", MaxConfigBytes+1))); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("oversized error = %v", err)
	}
	var options strings.Builder
	for index := 0; index <= MaxConfigOptions; index++ {
		options.WriteString("key = value\n")
	}
	if _, err := ParseConfig([]byte(options.String())); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("option limit error = %v", err)
	}
}

func TestSplitMonitorSeedsDropsLegacyAlternative(t *testing.T) {
	got := splitMonitorSeeds("[v2:192.0.2.1:3300/0,v1:192.0.2.1:6789/0] 192.0.2.2")
	if len(got) != 2 || got[0] != "v2:192.0.2.1:3300/0" || got[1] != "192.0.2.2" {
		t.Fatalf("seeds = %v", got)
	}
}
