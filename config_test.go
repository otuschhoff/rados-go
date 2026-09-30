package rados

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	if config.Entity != "client.admin" || config.SecurityMode != SecurityModeSecure || config.DialTimeout != 10*time.Second || config.HandshakeTimeout != 15*time.Second || config.OperationTimeout != 30*time.Second || config.MaxSessions != 256 || config.MaxReceiveBytes != 256<<20 || config.MaxQueuedReceiveBytes != 64<<20 {
		t.Fatalf("defaults = %+v", config)
	}
	if cluster, ok := config.Option("cluster"); !ok || cluster != "ceph" {
		t.Fatalf("cluster = %q, %t", cluster, ok)
	}
}

func TestReceiveConfigOptions(t *testing.T) {
	for _, option := range []struct {
		name         string
		defaultValue string
	}{
		{"max_sessions", "256"},
		{"max_receive_bytes", "268435456"},
		{"max_queued_receive_bytes", "67108864"},
	} {
		t.Run(option.name, func(t *testing.T) {
			for _, config := range []Config{{}, DefaultConfig()} {
				value, ok := config.Option(option.name)
				if !ok || value != option.defaultValue {
					t.Fatalf("default Option = %q, %t; want %q", value, ok, option.defaultValue)
				}
			}
			for _, alias := range []string{option.name, strings.ReplaceAll(option.name, "_", "-"), strings.ReplaceAll(option.name, "_", " ")} {
				config, err := (Config{}).WithOption(alias, "123")
				if err != nil {
					t.Fatal(err)
				}
				if value, ok := config.Option(option.name); !ok || value != "123" {
					t.Fatalf("Option(%q) = %q, %t", alias, value, ok)
				}
			}
			invalid := []string{"", "0", "-1", "+1", " 1", "1 ", "1 2", "1.5", "1MiB", "18446744073709551616"}
			if option.name == "max_sessions" {
				invalid = append(invalid, strconv.FormatUint(uint64(^uint(0)>>1)+1, 10))
			}
			for _, value := range invalid {
				if _, err := DefaultConfig().WithOption(option.name, value); !errors.Is(err, ErrInvalidArgument) {
					t.Fatalf("WithOption(%q, %q) error = %v", option.name, value, err)
				}
			}
		})
	}
}

func TestReceiveConfigParsing(t *testing.T) {
	config, err := ParseConfig([]byte("[global]\nmax sessions = 12\nmax receive bytes = 1024\nmax queued receive bytes = 256\n[client.admin]\nmax_sessions = 8\n"))
	if err != nil {
		t.Fatal(err)
	}
	if config.MaxSessions != 8 || config.MaxReceiveBytes != 1024 || config.MaxQueuedReceiveBytes != 256 {
		t.Fatal("file receive limits differ")
	}
	configured, remainder, err := config.ParseArgs([]string{"--max-sessions=4", "--max-receive-bytes", "2048", "--max-queued-receive-bytes=512", "--future-receive-limit=9", "input"})
	if err != nil {
		t.Fatal(err)
	}
	if configured.MaxSessions != 4 || configured.MaxReceiveBytes != 2048 || configured.MaxQueuedReceiveBytes != 512 || !reflect.DeepEqual(remainder, []string{"--future-receive-limit=9", "input"}) {
		t.Fatal("argument receive limits or remainder differ")
	}
	if config.MaxSessions != 8 || config.MaxReceiveBytes != 1024 || config.MaxQueuedReceiveBytes != 256 {
		t.Fatal("argument parsing mutated its input")
	}
	for _, name := range []string{"max_sessions", "max_receive_bytes", "max_queued_receive_bytes"} {
		for _, value := range []string{"0", "-1", "1.5", "18446744073709551616"} {
			if _, err := ParseConfig([]byte("[global]\n" + name + "=" + value + "\n")); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("file option %s=%s error = %v", name, value, err)
			}
			if _, _, err := config.ParseArgs([]string{"--" + strings.ReplaceAll(name, "_", "-") + "=" + value}); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("argument option %s=%s error = %v", name, value, err)
			}
		}
		if _, _, err := config.ParseArgs([]string{"--" + strings.ReplaceAll(name, "_", "-")}); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("missing argument %s error = %v", name, err)
		}
	}
}

func TestReceiveConfigEnv(t *testing.T) {
	t.Setenv("GO_LIBRADOS_MAX_SESSIONS", "0")
	t.Setenv("RECEIVE_TEST_MAX_SESSIONS", "7")
	t.Setenv("RECEIVE_TEST_MAX_RECEIVE_BYTES", "4096")
	t.Setenv("RECEIVE_TEST_MAX_QUEUED_RECEIVE_BYTES", "1024")
	t.Setenv("RECEIVE_TEST_FUTURE_RECEIVE_LIMIT", "0")
	config, err := (Config{}).ParseEnv("RECEIVE_TEST_")
	if err != nil {
		t.Fatal(err)
	}
	if config.MaxSessions != 7 || config.MaxReceiveBytes != 4096 || config.MaxQueuedReceiveBytes != 1024 {
		t.Fatal("environment receive limits differ")
	}
	if _, ok := config.Option("future_receive_limit"); ok {
		t.Fatal("unrecognized environment option was applied")
	}
	for _, suffix := range []string{"MAX_SESSIONS", "MAX_RECEIVE_BYTES", "MAX_QUEUED_RECEIVE_BYTES"} {
		t.Run(suffix, func(t *testing.T) {
			for _, value := range []string{"0", "-1", " 2", "18446744073709551616"} {
				t.Setenv("RECEIVE_TEST_"+suffix, value)
				if _, err := config.ParseEnv("RECEIVE_TEST"); !errors.Is(err, ErrInvalidArgument) {
					t.Fatalf("environment %s error = %v", suffix, err)
				}
			}
		})
	}
}

func TestReceiveConfigSetterOrderAndUint64Range(t *testing.T) {
	for _, options := range [][]string{
		{"max_receive_bytes", "max_queued_receive_bytes"},
		{"max_queued_receive_bytes", "max_receive_bytes"},
	} {
		config := DefaultConfig()
		config.Monitors = []string{"127.0.0.1:3300"}
		config.Key = []byte(testPublicKey)
		for _, name := range options {
			var err error
			config, err = config.WithOption(name, "1024")
			if err != nil {
				t.Fatal(err)
			}
		}
		client, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		_ = client.Close()
	}
	config, err := (Config{}).WithOption("max_sessions", strconv.FormatUint(uint64(^uint(0)>>1), 10))
	if err != nil || config.MaxSessions != int(^uint(0)>>1) {
		t.Fatalf("maximum int session count error = %v", err)
	}
	for _, name := range []string{"max_receive_bytes", "max_queued_receive_bytes"} {
		config, err = config.WithOption(name, "18446744073709551615")
		if err != nil {
			t.Fatal(err)
		}
		if value, ok := config.Option(name); !ok || value != "18446744073709551615" {
			t.Fatalf("uint64 maximum Option(%s) = %q, %t", name, value, ok)
		}
	}
}

func TestParseConfigSelectsEntityAndAppliesKnownOptions(t *testing.T) {
	data := []byte("[global]\ncluster = test\nname = client.test\nmon host = v2:192.0.2.1:3300/0 # preferred\nms mode = crc\ndial timeout = 3s\nunknown = ignored\n[client.other]\nmon_host = 192.0.2.9\n[client.test]\nmon_host = 192.0.2.2:3300\noperation_timeout = 9s\nrados_osd_op_timeout = 4\n")
	config, err := ParseConfig(data)
	if err != nil {
		t.Fatal(err)
	}
	if config.Entity != "client.test" || !reflect.DeepEqual(config.Monitors, []string{"192.0.2.2:3300"}) || config.SecurityMode != SecurityModeCRC || config.DialTimeout != 3*time.Second || config.OperationTimeout != 4*time.Second {
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
		"[global]\noperation_timeout = -1s\n",
		"[global]\nrados_osd_op_timeout = -1\n",
		"[global]\ninclude = /etc/ceph/other.conf\n",
	} {
		if _, err := ParseConfig([]byte(data)); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("ParseConfig(%q) error = %v", data, err)
		}
	}
}

func TestOperationTimeoutOptionsAcceptZeroAndAliasFormats(t *testing.T) {
	config, err := ParseConfig([]byte("[global]\noperation_timeout = 0\n"))
	if err != nil || config.OperationTimeout != 0 {
		t.Fatalf("zero operation_timeout config=%+v error=%v", config, err)
	}
	config, err = config.WithOption("rados_osd_op_timeout", "250ms")
	if err != nil || config.OperationTimeout != 250*time.Millisecond {
		t.Fatalf("duration alias config=%+v error=%v", config, err)
	}
	for _, name := range []string{"operation_timeout", "rados_osd_op_timeout"} {
		value, ok := (Config{}).Option(name)
		if !ok || value != "0s" {
			t.Fatalf("Option(%q)=%q,%t", name, value, ok)
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
	config, remainder, err := DefaultConfig().ParseArgs([]string{"input", "--unknown", "value", "--id=test", "--mon-host", "192.0.2.1:3300", "--operation-timeout=4s", "--rados-osd-op-timeout", "2", "--", "--cluster=ignored"})
	if err != nil {
		t.Fatal(err)
	}
	if config.Entity != "client.test" || config.OperationTimeout != 2*time.Second || !reflect.DeepEqual(config.Monitors, []string{"192.0.2.1:3300"}) {
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
	t.Setenv("CUSTOM_OPERATION_TIMEOUT", "8s")
	t.Setenv("CUSTOM_RADOS_OSD_OP_TIMEOUT", "1")
	config, err := DefaultConfig().ParseEnv("CUSTOM")
	if err != nil {
		t.Fatal(err)
	}
	if config.Entity != "client.custom" || config.HandshakeTimeout != 7*time.Second || config.OperationTimeout != 8*time.Second || !reflect.DeepEqual(config.Monitors, []string{"192.0.2.4"}) {
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
