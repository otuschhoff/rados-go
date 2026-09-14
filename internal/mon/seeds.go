package mon

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

const DefaultMonitorV2Port uint16 = 3300

type Endpoint struct {
	Address       netip.AddrPort
	EntityAddress protocol.EntityAddr
	priority      uint16
	weight        uint16
}

type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
	LookupSRV(context.Context, string, string, string) (string, []*net.SRV, error)
}

type SeedLimits struct {
	MaxSeeds     uint32
	MaxAddresses uint32
}

func ResolveSeeds(ctx context.Context, seeds []string, resolver Resolver, limits SeedLimits) ([]Endpoint, error) {
	if len(seeds) == 0 || limits.MaxSeeds == 0 || limits.MaxAddresses == 0 || uint64(len(seeds)) > uint64(limits.MaxSeeds) {
		return nil, wire.ErrLimitExceeded
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	result := make([]Endpoint, 0, len(seeds))
	seen := make(map[netip.AddrPort]struct{})
	appendEndpoint := func(address netip.AddrPort) error {
		address = netip.AddrPortFrom(address.Addr().Unmap(), address.Port())
		if _, exists := seen[address]; exists {
			return nil
		}
		if uint64(len(result)) >= uint64(limits.MaxAddresses) {
			return wire.ErrLimitExceeded
		}
		entity, err := entityAddress(address)
		if err != nil {
			return err
		}
		seen[address] = struct{}{}
		result = append(result, Endpoint{Address: address, EntityAddress: entity})
		return nil
	}
	for _, seed := range seeds {
		seed = strings.TrimSpace(seed)
		if strings.HasPrefix(seed, "dns-srv:") {
			name := strings.TrimSuffix(strings.TrimPrefix(seed, "dns-srv:"), ".")
			if name == "" {
				return nil, fmt.Errorf("%w: empty monitor SRV name", wire.ErrMalformed)
			}
			_, records, err := resolver.LookupSRV(ctx, "ceph-mon", "tcp", name)
			if err != nil {
				return nil, err
			}
			if uint64(len(records)) > uint64(limits.MaxAddresses) {
				return nil, wire.ErrLimitExceeded
			}
			for _, record := range records {
				host := strings.TrimSuffix(record.Target, ".")
				if err := resolveHost(ctx, resolver, host, record.Port, limits.MaxAddresses, appendEndpoint); err != nil {
					return nil, err
				}
			}
			continue
		}
		host, port, err := parseSeed(seed)
		if err != nil {
			return nil, err
		}
		if address, err := netip.ParseAddr(host); err == nil {
			if err := appendEndpoint(netip.AddrPortFrom(address, port)); err != nil {
				return nil, err
			}
			continue
		}
		if err := resolveHost(ctx, resolver, host, port, limits.MaxAddresses, appendEndpoint); err != nil {
			return nil, err
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("%w: monitor seeds resolved to no addresses", wire.ErrMalformed)
	}
	return result, nil
}

func parseSeed(seed string) (string, uint16, error) {
	seed = strings.TrimSpace(seed)
	if strings.HasPrefix(seed, "v2:") {
		seed = strings.TrimPrefix(seed, "v2:")
	} else if strings.HasPrefix(seed, "v1:") {
		return "", 0, fmt.Errorf("%w: messenger v1 monitor seed", wire.ErrUnsupportedVersion)
	}
	if slash := strings.LastIndexByte(seed, '/'); slash >= 0 {
		if _, err := strconv.ParseUint(seed[slash+1:], 10, 32); err != nil {
			return "", 0, fmt.Errorf("%w: invalid monitor nonce", wire.ErrMalformed)
		}
		seed = seed[:slash]
	}
	if address, err := netip.ParseAddr(seed); err == nil {
		return address.String(), DefaultMonitorV2Port, nil
	}
	if host, portText, err := net.SplitHostPort(seed); err == nil {
		port, err := strconv.ParseUint(portText, 10, 16)
		if err != nil || port == 0 {
			return "", 0, fmt.Errorf("%w: invalid monitor port", wire.ErrMalformed)
		}
		return strings.Trim(host, "[]"), uint16(port), nil
	}
	if seed == "" || strings.Contains(seed, ":") {
		return "", 0, fmt.Errorf("%w: invalid monitor seed %q", wire.ErrMalformed, seed)
	}
	return seed, DefaultMonitorV2Port, nil
}

func resolveHost(ctx context.Context, resolver Resolver, host string, port uint16, maximum uint32, appendEndpoint func(netip.AddrPort) error) error {
	addresses, err := resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return err
	}
	if uint64(len(addresses)) > uint64(maximum) {
		return wire.ErrLimitExceeded
	}
	for _, address := range addresses {
		if !address.IsValid() || address.IsUnspecified() {
			continue
		}
		if err := appendEndpoint(netip.AddrPortFrom(address, port)); err != nil {
			return err
		}
	}
	return nil
}

func entityAddress(endpoint netip.AddrPort) (protocol.EntityAddr, error) {
	if endpoint.Addr().Unmap().Is4() {
		return protocol.IPv4EntityAddr(protocol.AddressV2, 0, endpoint)
	}
	return protocol.IPv6EntityAddr(protocol.AddressV2, 0, endpoint, 0, 0)
}
