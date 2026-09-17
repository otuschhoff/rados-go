package mon

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
)

const (
	MaxConfigBytes   = 1 << 20
	MaxConfigOptions = 256
)

type BootstrapConfig struct {
	ClusterName  string
	Entity       string
	MonitorSeeds []string
	KeyringPath  string
	ExpectedFSID *maps.FSID
}

func DefaultBootstrapConfig() BootstrapConfig {
	return BootstrapConfig{ClusterName: "ceph", Entity: "client.admin"}
}

// LoadBootstrapConfig applies defaults, an explicitly named Ceph config file,
// the opt-in GO_LIBRADOS_* environment overlay, and explicit options, in order.
func LoadBootstrapConfig(path string, options BootstrapConfig, loadEnvironment bool) (BootstrapConfig, error) {
	result := DefaultBootstrapConfig()
	entity := result.Entity
	if loadEnvironment && os.Getenv("GO_LIBRADOS_ENTITY") != "" {
		entity = os.Getenv("GO_LIBRADOS_ENTITY")
	}
	if options.Entity != "" {
		entity = options.Entity
	}
	if path != "" {
		sections, err := parseConfigFile(path)
		if err != nil {
			return BootstrapConfig{}, err
		}
		if err := applyConfigSection(&result, sections["global"]); err != nil {
			return BootstrapConfig{}, err
		}
		if err := applyConfigSection(&result, sections[entity]); err != nil {
			return BootstrapConfig{}, err
		}
	}
	if loadEnvironment {
		if err := applyEnvironment(&result); err != nil {
			return BootstrapConfig{}, err
		}
	}
	applyBootstrapOptions(&result, options)
	if result.ClusterName == "" || result.Entity == "" || len(result.MonitorSeeds) == 0 {
		return BootstrapConfig{}, fmt.Errorf("%w: cluster, entity, and monitor seeds are required", wire.ErrMalformed)
	}
	result.KeyringPath = strings.ReplaceAll(result.KeyringPath, "$cluster", result.ClusterName)
	result.KeyringPath = strings.ReplaceAll(result.KeyringPath, "$name", result.Entity)
	return result, nil
}

func parseConfigFile(path string) (map[string]map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, MaxConfigBytes+1))
	if err != nil {
		return nil, err
	}
	return ParseConfig(data)
}

// ParseConfig parses the bounded Ceph configuration subset used for monitor
// bootstrap. It returns normalized keys grouped by exact section name.
func ParseConfig(data []byte) (map[string]map[string]string, error) {
	if len(data) > MaxConfigBytes {
		return nil, fmt.Errorf("%w: config size limit", wire.ErrMalformed)
	}
	sections := map[string]map[string]string{"global": {}}
	section := "global"
	optionCount := 0
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	buffer := make([]byte, 4096)
	scanner.Buffer(buffer, MaxConfigBytes)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := stripConfigComment(strings.TrimSpace(scanner.Text()))
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") {
			if !strings.HasSuffix(line, "]") || strings.Count(line, "[") != 1 || strings.Count(line, "]") != 1 {
				return nil, fmt.Errorf("%w: malformed section at line %d", wire.ErrMalformed, lineNumber)
			}
			section = strings.TrimSpace(line[1 : len(line)-1])
			if section == "" {
				return nil, fmt.Errorf("%w: empty section at line %d", wire.ErrMalformed, lineNumber)
			}
			if sections[section] == nil {
				sections[section] = make(map[string]string)
			}
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%w: config line %d", wire.ErrMalformed, lineNumber)
		}
		key = strings.Join(strings.Fields(strings.ToLower(key)), "_")
		value = strings.TrimSpace(value)
		if key == "" {
			return nil, fmt.Errorf("%w: empty key at line %d", wire.ErrMalformed, lineNumber)
		}
		optionCount++
		if optionCount > MaxConfigOptions {
			return nil, fmt.Errorf("%w: config option limit", wire.ErrMalformed)
		}
		sections[section][key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%w: config scanner: %v", wire.ErrMalformed, err)
	}
	return sections, nil
}

func stripConfigComment(line string) string {
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

func applyConfigSection(config *BootstrapConfig, values map[string]string) error {
	if value := values["cluster"]; value != "" {
		config.ClusterName = value
	}
	if value := values["mon_host"]; value != "" {
		config.MonitorSeeds = splitMonitorSeeds(value)
	}
	if value := values["keyring"]; value != "" {
		config.KeyringPath = value
	}
	if value := values["fsid"]; value != "" {
		fsid, err := parseFSID(value)
		if err != nil {
			return err
		}
		config.ExpectedFSID = &fsid
	}
	return nil
}

func applyEnvironment(config *BootstrapConfig) error {
	if value := os.Getenv("GO_LIBRADOS_CLUSTER"); value != "" {
		config.ClusterName = value
	}
	if value := os.Getenv("GO_LIBRADOS_ENTITY"); value != "" {
		config.Entity = value
	}
	if value := os.Getenv("GO_LIBRADOS_MON_HOST"); value != "" {
		config.MonitorSeeds = splitMonitorSeeds(value)
	}
	if value := os.Getenv("GO_LIBRADOS_KEYRING"); value != "" {
		config.KeyringPath = value
	}
	if value := os.Getenv("GO_LIBRADOS_FSID"); value != "" {
		fsid, err := parseFSID(value)
		if err != nil {
			return err
		}
		config.ExpectedFSID = &fsid
	}
	return nil
}

func applyBootstrapOptions(config *BootstrapConfig, options BootstrapConfig) {
	if options.ClusterName != "" {
		config.ClusterName = options.ClusterName
	}
	if options.Entity != "" {
		config.Entity = options.Entity
	}
	if options.MonitorSeeds != nil {
		config.MonitorSeeds = append([]string(nil), options.MonitorSeeds...)
	}
	if options.KeyringPath != "" {
		config.KeyringPath = options.KeyringPath
	}
	if options.ExpectedFSID != nil {
		fsid := *options.ExpectedFSID
		config.ExpectedFSID = &fsid
	}
}

func splitMonitorSeeds(value string) []string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		value = value[1 : len(value)-1]
	}
	fields := strings.FieldsFunc(value, func(character rune) bool {
		return character == ',' || character == ';' || character == ' ' || character == '\t'
	})
	result := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.Trim(field, "[]")
		if strings.HasPrefix(field, "v1:") {
			continue
		}
		result = append(result, field)
	}
	return result
}

func parseFSID(value string) (maps.FSID, error) {
	compact := strings.ReplaceAll(strings.TrimSpace(value), "-", "")
	decoded, err := hex.DecodeString(compact)
	if err != nil || len(decoded) != 16 {
		return maps.FSID{}, fmt.Errorf("%w: invalid fsid", wire.ErrMalformed)
	}
	var fsid maps.FSID
	copy(fsid[:], decoded)
	return fsid, nil
}
