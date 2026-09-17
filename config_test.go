package rados

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	if config.Entity != "client.admin" || config.SecurityMode != SecurityModeSecure || config.DialTimeout != 10*time.Second || config.HandshakeTimeout != 15*time.Second || config.OperationTimeout != 30*time.Second {
		t.Fatalf("defaults = %+v", config)
	}
	if cluster, ok := config.Option("cluster"); !ok || cluster != "ceph" {
		t.Fatalf("cluster = %q, %t", cluster, ok)
	}
}

func TestParseConfigSelectsEntityAndAppliesKnownOptions(t *testing.T) {
	data := []byte("[global]\ncluster = test\nname = client.test\nmon host = v2:192.0.2.1:3300/0 # preferred\nms mode = crc\ndial timeout = 3s\nunknown = ignored\n[client.other]\nmon_host = 192.0.2.9\n[client.test]\nmon_host = 192.0.2.2:3300\noperation_timeout = 9s\n")
	config, err := ParseConfig(data)
	if err != nil {
		t.Fatal(err)
	}
	if config.Entity != "client.test" || !reflect.DeepEqual(config.Monitors, []string{"192.0.2.2:3300"}) || config.SecurityMode != SecurityModeCRC || config.DialTimeout != 3*time.Second || config.OperationTimeout != 9*time.Second {
		t.Fatalf("config = %+v", config)
	}
	if _, ok := config.Option("unknown"); ok {
		t.Fatal("unknown file option was retained")
	}
}

func TestConfigRejectsMalformedKnownValuesAndIncludes(t *testing.T) {
	for _, data := range []string{
		"[global]\nms_mode = plaintext\n",
		"[global]\nfsid = no\n",
		"[global]\ndial_timeout = forever\n",
		"[global]\ninclude = /etc/ceph/other.conf\n",
	} {
		if _, err := ParseConfig([]byte(data)); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("ParseConfig(%q) error = %v", data, err)
		}
	}
}

func TestWithOptionIsImmutableAndRetainsUnknown(t *testing.T) {
	base := DefaultConfig()
	base.Monitors = []string{"192.0.2.1"}
	configured, err := base.WithOption("mon-host", "192.0.2.2,192.0.2.3")
	if err != nil {
		t.Fatal(err)
	}
	configured, err = configured.WithOption("future_option", "enabled")
	if err != nil {
		t.Fatal(err)
	}
	configured.Monitors[0] = "changed"
	if base.Monitors[0] != "192.0.2.1" {
		t.Fatalf("base monitors changed: %v", base.Monitors)
	}
	if value, ok := configured.Option("future-option"); !ok || value != "enabled" {
		t.Fatalf("unknown option = %q, %t", value, ok)
	}
	second, err := configured.WithOption("another", "value")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := configured.Option("another"); ok {
		t.Fatal("option map was shared")
	}
	if value, ok := second.Option("another"); !ok || value != "value" {
		t.Fatalf("second option = %q, %t", value, ok)
	}
}

func TestParseArgsPrecedenceAndRemainder(t *testing.T) {
	config, remainder, err := DefaultConfig().ParseArgs([]string{"input", "--unknown", "value", "--id=test", "--mon-host", "192.0.2.1:3300", "--operation-timeout=4s", "--", "--cluster=ignored"})
	if err != nil {
		t.Fatal(err)
	}
	if config.Entity != "client.test" || config.OperationTimeout != 4*time.Second || !reflect.DeepEqual(config.Monitors, []string{"192.0.2.1:3300"}) {
		t.Fatalf("config = %+v", config)
	}
	if !reflect.DeepEqual(remainder, []string{"input", "--unknown", "value", "--", "--cluster=ignored"}) {
		t.Fatalf("remainder = %q", remainder)
	}
}

func TestParseEnvUsesOnlyExplicitPrefix(t *testing.T) {
	t.Setenv("GO_LIBRADOS_ENTITY", "client.default")
	t.Setenv("CUSTOM_ENTITY", "client.custom")
	t.Setenv("CUSTOM_MON_HOST", "192.0.2.4")
	t.Setenv("CUSTOM_HANDSHAKE_TIMEOUT", "7s")
	config, err := DefaultConfig().ParseEnv("CUSTOM")
	if err != nil {
		t.Fatal(err)
	}
	if config.Entity != "client.custom" || config.HandshakeTimeout != 7*time.Second || !reflect.DeepEqual(config.Monitors, []string{"192.0.2.4"}) {
		t.Fatalf("config = %+v", config)
	}
}

func TestLoadConfigExpandsAndLoadsKeyring(t *testing.T) {
	directory := t.TempDir()
	keyringDirectory := filepath.Join(directory, "test")
	if err := os.Mkdir(keyringDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	keyringPath := filepath.Join(keyringDirectory, "client.test.keyring")
	if err := os.WriteFile(keyringPath, []byte("[client.test]\n key = '"+testPublicKey+"' ; generated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "ceph.conf")
	data := "[global]\ncluster = test\nentity = client.test\nmon_host = 192.0.2.1\nkeyring = " + directory + "/$cluster/$name.keyring\n"
	if err := os.WriteFile(configPath, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(config.Key) != testPublicKey {
		t.Fatal("loaded key differs from canonical encoded key")
	}
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	config.Key[0] = 'x'
	if string(client.config.Key) != testPublicKey {
		t.Fatal("New retained caller key storage")
	}
}

func TestExplicitKeyringOptionLoadsAfterEntity(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "client.test.keyring")
	if err := os.WriteFile(path, []byte("[client.test]\nkey = "+testPublicKey+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config, remainder, err := DefaultConfig().ParseArgs([]string{"--keyring", directory + "/$name.keyring", "--name", "client.test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(remainder) != 0 || string(config.Key) != testPublicKey {
		t.Fatalf("remainder/key length = %q/%d", remainder, len(config.Key))
	}
	configured, err := DefaultConfig().WithOption("name", "client.test")
	if err != nil {
		t.Fatal(err)
	}
	configured, err = configured.WithOption("keyring", path)
	if err != nil || string(configured.Key) != testPublicKey {
		t.Fatalf("WithOption keyring error/key length = %v/%d", err, len(configured.Key))
	}
}

func TestLoadConfigAndKeyringAreBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversized")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", maxConfigBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("LoadConfig error = %v", err)
	}
	if _, err := LoadKeyring(path, "client.test"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("LoadKeyring error = %v", err)
	}
}
