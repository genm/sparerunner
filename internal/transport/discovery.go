package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"time"

	"github.com/hashicorp/mdns"
)

const DiscoveryService = "_tewake._tcp"

type EndpointCandidate struct{ Address string }

// Discoverer supplies unauthenticated endpoint candidates only. Every caller
// must pair its result with PinnedControllerTLSConfig before enrollment.
type Discoverer interface {
	Discover(context.Context) ([]EndpointCandidate, error)
}
type Advertiser interface{ Close() error }

type MDNSDiscoverer struct{ Timeout time.Duration }

func (discoverer MDNSDiscoverer) Discover(ctx context.Context) ([]EndpointCandidate, error) {
	timeout := discoverer.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	entries := make(chan *mdns.ServiceEntry, 16)
	query := mdns.QueryParam{Service: DiscoveryService, Domain: "local", Timeout: timeout, Entries: entries}
	done := make(chan error, 1)
	go func() { done <- mdns.Query(&query); close(entries) }()
	candidateSet := map[string]struct{}{}
	add := func(entry *mdns.ServiceEntry) {
		if candidate, ok := candidateFor(entry); ok {
			candidateSet[candidate.Address] = struct{}{}
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-done:
			if err != nil {
				return nil, err
			}
			for entry := range entries {
				add(entry)
			}
			return stableCandidates(candidateSet), nil
		case entry, ok := <-entries:
			if !ok {
				err := <-done
				return stableCandidates(candidateSet), err
			}
			add(entry)
		}
	}
}

func candidateFor(entry *mdns.ServiceEntry) (EndpointCandidate, bool) {
	if entry == nil || entry.Port < 1 || entry.Port > 65535 {
		return EndpointCandidate{}, false
	}
	if address, ok := netip.AddrFromSlice(entry.AddrV4); ok && !address.IsUnspecified() {
		return EndpointCandidate{Address: net.JoinHostPort(address.String(), strconv.Itoa(entry.Port))}, true
	}
	var ip net.IP
	zone := ""
	if entry.AddrV6IPAddr != nil {
		ip, zone = entry.AddrV6IPAddr.IP, entry.AddrV6IPAddr.Zone
	} else {
		ip = entry.AddrV6
	}
	address, ok := netip.AddrFromSlice(ip)
	if !ok || address.IsUnspecified() || (address.IsLinkLocalUnicast() && zone == "") {
		return EndpointCandidate{}, false
	}
	host := address.String()
	if zone != "" {
		host += "%" + zone
	}
	return EndpointCandidate{Address: net.JoinHostPort(host, strconv.Itoa(entry.Port))}, true
}

func stableCandidates(candidates map[string]struct{}) []EndpointCandidate {
	addresses := make([]string, 0, len(candidates))
	for address := range candidates {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	result := make([]EndpointCandidate, len(addresses))
	for index, address := range addresses {
		result[index] = EndpointCandidate{Address: address}
	}
	return result
}

type MDNSAdvertiser struct{ server *mdns.Server }

func StartMDNSAdvertiser(instance string, port int, addresses []net.IP) (*MDNSAdvertiser, error) {
	if instance == "" || port < 1 || port > 65535 {
		return nil, errors.New("invalid mDNS advertisement")
	}
	service, err := mdns.NewMDNSService(instance, DiscoveryService, "local.", "", port, addresses, nil)
	if err != nil {
		return nil, fmt.Errorf("create mDNS service: %w", err)
	}
	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		return nil, fmt.Errorf("start mDNS service: %w", err)
	}
	return &MDNSAdvertiser{server: server}, nil
}

func (advertiser *MDNSAdvertiser) Close() error {
	if advertiser == nil || advertiser.server == nil {
		return nil
	}
	advertiser.server.Shutdown()
	return nil
}
