package rados

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/otuschhoff/go-librados/internal/cephx"
	"github.com/otuschhoff/go-librados/internal/mon"
)

const (
	maxConfigBytes   = 1 << 20
	maxConfigOptions = 256
	maxMonitorSeeds  = 64
)

// DefaultConfig returns the finite-timeout defaults without reading files,
// command-line flags, or the process environment.
func DefaultConfig() Config {
	return Config{
		Entity:           "client.admin",
		SecurityMode:     SecurityModeSecure,
		DialTimeout:      defaultDialTimeout,
		HandshakeTimeout: defaultHandshakeTimeout,
		OperationTimeout: defaultOperationTimeout,
		cluster:          "ceph",
	}
}

// ParseConfig parses the supported Ceph configuration subset from data.
func ParseConfig(data []byte) (Config, error) {
	sections, err := mon.ParseConfig(data)
	if err != nil {
		return Config{}, invalidConfig("config", err)
	}
	config := DefaultConfig()
	if err := config.applySection(sections["global"]); err != nil {
		return Config{}, err
	}
	if err := config.applySection(sections[config.Entity]); err != nil {
		return Config{}, err
	}
	return config.clone(), nil
}

// LoadConfig reads and parses path and loads its selected keyring when no
// direct key was configured.
func LoadConfig(path string) (Config, error) {
	data, err := readBoundedFile(path, maxConfigBytes)
	if err != nil {
		return Config{}, err
	}
	config, err := ParseConfig(data)
	if err != nil {
		return Config{}, err
	}
	if config.keyring != "" && len(config.Key) == 0 {
		keyringPath := strings.ReplaceAll(config.keyring, "$cluster", config.clusterName())
		keyringPath = strings.ReplaceAll(keyringPath, "$name", config.Entity)
		key, err := LoadKeyring(keyringPath, config.Entity)
		if err != nil {
			return Config{}, err
		}
		config.Key = key
	}
	return config.clone(), nil
}

// LoadKeyring returns a copied canonical encoded key for entity.
func LoadKeyring(path, entity string) ([]byte, error) {
	if entity == "" {
		entity = DefaultConfig().Entity
	}
	data, err := readBoundedFile(path, cephx.DefaultMaxKeyring)
	if err != nil {
		return nil, err
	}
	if _, err := cephx.ParseKeyring(data, entity, cephx.DefaultMaxKeyring); err != nil {
		return nil, invalidConfig("keyring", err)
	}
	encoded, err := encodedKeyFromKeyring(data, entity)
	if err != nil {
		return nil, invalidConfig("keyring", err)
	}
	return append([]byte(nil), encoded...), nil
}

// WithOption returns a deep-copied Config with one option applied.
func (config Config) WithOption(name, value string) (Config, error) {
	return config.withOption(name, value, true)
}

func (config Config) withOption(name, value string, loadKeyring bool) (Config, error) {
	config = config.clone()
	name = normalizeOptionName(name)
	if name == "" {
		return Config{}, invalidConfig("option", errors.New("empty option name"))
	}
	value = strings.TrimSpace(value)
	switch name {
	case "cluster":
		if !validSimpleName(value) {
			return Config{}, invalidConfig(name, errors.New("invalid cluster name"))
		}
		config.cluster = value
	case "entity", "name":
		if !validClientEntity(value) {
			return Config{}, invalidConfig(name, errors.New("invalid client entity"))
		}
		config.Entity = value
	case "mon_host":
		if strings.ContainsAny(value, "\x00\r\n") {
			return Config{}, invalidConfig(name, errors.New("invalid monitor seeds"))
		}
		monitors := splitMonitorSeeds(value)
		if len(monitors) == 0 || len(monitors) > maxMonitorSeeds {
			return Config{}, invalidConfig(name, errors.New("invalid monitor seed count"))
		}
		config.Monitors = monitors
	case "fsid":
		if _, err := parsePublicFSID(value); err != nil {
			return Config{}, invalidConfig(name, err)
		}
		config.ClusterFSID = value
	case "key":
		if _, err := cephx.ParseKey(config.entityName(), value, cephx.DefaultMaxKeyBytes); err != nil {
			return Config{}, invalidConfig(name, err)
		}
		config.Key = append([]byte(nil), value...)
	case "keyring":
		if value == "" || strings.ContainsAny(value, "\x00\r\n") {
			return Config{}, invalidConfig(name, errors.New("invalid keyring path"))
		}
		config.keyring = value
		if loadKeyring {
			if err := config.loadConfiguredKeyring(); err != nil {
				return Config{}, err
			}
		}
	case "ms_mode":
		switch strings.ToLower(value) {
		case "secure":
			config.SecurityMode = SecurityModeSecure
		case "crc":
			config.SecurityMode = SecurityModeCRC
		default:
			return Config{}, invalidConfig(name, errors.New("mode must be secure or crc"))
		}
	case "dial_timeout":
		duration, err := parsePositiveDuration(name, value)
		if err != nil {
			return Config{}, err
		}
		config.DialTimeout = duration
	case "handshake_timeout":
		duration, err := parsePositiveDuration(name, value)
		if err != nil {
			return Config{}, err
		}
		config.HandshakeTimeout = duration
	case "operation_timeout":
		duration, err := parsePositiveDuration(name, value)
		if err != nil {
			return Config{}, err
		}
		config.OperationTimeout = duration
	default:
		if config.options == nil {
			config.options = make(map[string]string)
		}
		if _, exists := config.options[name]; !exists && len(config.options) >= maxConfigOptions {
			return Config{}, invalidConfig(name, errors.New("option count limit"))
		}
		config.options[name] = value
	}
	return config, nil
}

// Option reports the effective value of a known or retained unknown option.
func (config Config) Option(name string) (string, bool) {
	switch normalizeOptionName(name) {
	case "cluster":
		return config.clusterName(), true
	case "entity", "name":
		return config.entityName(), true
	case "mon_host":
		if config.Monitors == nil {
			return "", false
		}
		return strings.Join(config.Monitors, ","), true
	case "fsid":
		return config.ClusterFSID, config.ClusterFSID != ""
	case "key":
		return string(config.Key), len(config.Key) != 0
	case "keyring":
		return config.keyring, config.keyring != ""
	case "ms_mode":
		if config.SecurityMode == SecurityModeCRC {
			return "crc", true
		}
		if config.SecurityMode == SecurityModeSecure {
			return "secure", true
		}
		return "", false
	case "dial_timeout":
		return durationOption(config.DialTimeout)
	case "handshake_timeout":
		return durationOption(config.HandshakeTimeout)
	case "operation_timeout":
		return durationOption(config.OperationTimeout)
	default:
		value, ok := config.options[normalizeOptionName(name)]
		return value, ok
	}
}

// ParseArgs applies recognized long options and returns every unknown or
// non-option argument in its original order.
func (config Config) ParseArgs(arguments []string) (Config, []string, error) {
	config = config.clone()
	remainder := make([]string, 0, len(arguments))
	optionCount := 0
	keySeen := false
	keyringSeen := false
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--" {
			remainder = append(remainder, arguments[index:]...)
			break
		}
		if !strings.HasPrefix(argument, "--") {
			remainder = append(remainder, argument)
			continue
		}
		nameValue := strings.TrimPrefix(argument, "--")
		name, value, hasValue := strings.Cut(nameValue, "=")
		normalized := normalizeOptionName(name)
		if !isArgumentOption(normalized) {
			remainder = append(remainder, argument)
			continue
		}
		optionCount++
		if optionCount > maxConfigOptions {
			return Config{}, nil, invalidConfig("arguments", errors.New("option count limit"))
		}
		if !hasValue {
			if index+1 >= len(arguments) {
				return Config{}, nil, invalidConfig(normalized, errors.New("missing option value"))
			}
			index++
			value = arguments[index]
		}
		if normalized == "id" {
			normalized = "entity"
			value = "client." + value
		}
		if normalized == "key" {
			keySeen = true
		} else if normalized == "keyring" {
			keyringSeen = true
		}
		var err error
		config, err = config.withOption(normalized, value, false)
		if err != nil {
			return Config{}, nil, err
		}
	}
	if keyringSeen && !keySeen {
		if err := config.loadConfiguredKeyring(); err != nil {
			return Config{}, nil, err
		}
	}
	return config, remainder, nil
}

// ParseEnv applies only variables under the explicitly selected prefix.
func (config Config) ParseEnv(name string) (Config, error) {
	config = config.clone()
	prefix := strings.TrimSpace(name)
	if prefix == "" {
		prefix = "GO_LIBRADOS"
	}
	prefix = strings.TrimSuffix(prefix, "_") + "_"
	variables := []struct {
		suffix string
		option string
	}{
		{"CLUSTER", "cluster"},
		{"ENTITY", "entity"},
		{"MON_HOST", "mon_host"},
		{"KEYRING", "keyring"},
		{"FSID", "fsid"},
		{"KEY", "key"},
		{"MS_MODE", "ms_mode"},
		{"DIAL_TIMEOUT", "dial_timeout"},
		{"HANDSHAKE_TIMEOUT", "handshake_timeout"},
		{"OPERATION_TIMEOUT", "operation_timeout"},
	}
	credentialOption := ""
	for _, variable := range variables {
		value, exists := os.LookupEnv(prefix + variable.suffix)
		if !exists {
			continue
		}
		if variable.option == "key" || variable.option == "keyring" {
			credentialOption = variable.option
		}
		var err error
		config, err = config.withOption(variable.option, value, false)
		if err != nil {
			return Config{}, err
		}
	}
	if credentialOption == "keyring" {
		if err := config.loadConfiguredKeyring(); err != nil {
			return Config{}, err
		}
	}
	return config, nil
}

func (config *Config) applySection(values map[string]string) error {
	if len(values) == 0 {
		return nil
	}
	if _, exists := values["include"]; exists {
		return invalidConfig("include", errors.New("includes are not supported"))
	}
	if _, exists := values["include_dir"]; exists {
		return invalidConfig("include_dir", errors.New("includes are not supported"))
	}
	ordered := []string{"cluster", "entity", "name", "mon_host", "fsid", "key", "keyring", "ms_mode", "dial_timeout", "handshake_timeout", "operation_timeout"}
	for _, name := range ordered {
		value, exists := values[name]
		if !exists {
			continue
		}
		configValue := unquoteConfigValue(value)
		updated, err := config.withOption(name, configValue, false)
		if err != nil {
			return err
		}
		*config = updated
	}
	return nil
}

func (config Config) clone() Config {
	config.Monitors = append([]string(nil), config.Monitors...)
	config.Key = append([]byte(nil), config.Key...)
	if config.options != nil {
		options := make(map[string]string, len(config.options))
		for name, value := range config.options {
			options[name] = value
		}
		config.options = options
	}
	return config
}

func (config Config) clusterName() string {
	if config.cluster == "" {
		return "ceph"
	}
	return config.cluster
}

func (config Config) entityName() string {
	if config.Entity == "" {
		return "client.admin"
	}
	return config.Entity
}

func (config *Config) loadConfiguredKeyring() error {
	path := strings.ReplaceAll(config.keyring, "$cluster", config.clusterName())
	path = strings.ReplaceAll(path, "$name", config.entityName())
	key, err := LoadKeyring(path, config.entityName())
	if err != nil {
		return err
	}
	config.Key = key
	return nil
}

func parsePositiveDuration(name, value string) (time.Duration, error) {
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, invalidConfig(name, errors.New("duration must be positive and finite"))
	}
	return duration, nil
}

func durationOption(duration time.Duration) (string, bool) {
	if duration == 0 {
		return "", false
	}
	return duration.String(), true
}

func normalizeOptionName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "-", "_")
	return strings.Join(strings.Fields(name), "_")
}

func isArgumentOption(name string) bool {
	switch name {
	case "name", "id", "cluster", "mon_host", "fsid", "key", "keyring", "ms_mode", "dial_timeout", "handshake_timeout", "operation_timeout":
		return true
	default:
		return false
	}
}

func validSimpleName(value string) bool {
	return value != "" && !strings.ContainsAny(value, "[]/\\\x00\r\n")
}

func validClientEntity(value string) bool {
	kind, id, ok := strings.Cut(value, ".")
	return ok && kind == "client" && id != "" && !strings.ContainsAny(id, "[]\x00\r\n")
}

func splitMonitorSeeds(value string) []string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		value = value[1 : len(value)-1]
	}
	fields := strings.FieldsFunc(value, func(character rune) bool {
		return character == ',' || character == ';' || character == ' ' || character == '\t'
	})
	monitors := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.Trim(field, "[]")
		if field != "" && !strings.HasPrefix(field, "v1:") {
			monitors = append(monitors, field)
		}
	}
	return monitors
}

func readBoundedFile(path string, limit int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, invalidConfig("file", errors.New("size limit exceeded"))
	}
	return data, nil
}

func encodedKeyFromKeyring(data []byte, entity string) ([]byte, error) {
	section := ""
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 1024), cephx.DefaultMaxKeyring)
	for scanner.Scan() {
		line := stripComment(strings.TrimSpace(scanner.Text()))
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(line[1 : len(line)-1])
			continue
		}
		if section != entity {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(name) != "key" {
			continue
		}
		value = unquoteConfigValue(strings.TrimSpace(value))
		return append([]byte(nil), value...), nil
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, errors.New("validated credential key not found")
}

func stripComment(line string) string {
	var quote byte
	for index := 0; index < len(line); index++ {
		switch line[index] {
		case '\'', '"':
			if quote == 0 {
				quote = line[index]
			} else if quote == line[index] {
				quote = 0
			}
		case '#', ';':
			if quote == 0 {
				return strings.TrimSpace(line[:index])
			}
		}
	}
	return line
}

func unquoteConfigValue(value string) string {
	if len(value) >= 2 && ((value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')) {
		return value[1 : len(value)-1]
	}
	return value
}

func invalidConfig(name string, err error) error {
	return fmt.Errorf("%w: %s: %v", ErrInvalidArgument, name, err)
}
