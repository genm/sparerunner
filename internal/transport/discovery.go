package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
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
	var candidates []EndpointCandidate
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-done:
			if err != nil {
				return nil, err
			}
			for entry := range entries {
				candidates = append(candidates, candidateFor(entry))
			}
			return candidates, nil
		case entry, ok := <-entries:
			if !ok {
				err := <-done
				return candidates, err
			}
			candidates = append(candidates, candidateFor(entry))
		}
	}
}

func candidateFor(entry *mdns.ServiceEntry) EndpointCandidate {
	host := entry.AddrV4.String()
	if entry.AddrV4 == nil && entry.AddrV6 != nil {
		host = "[" + entry.AddrV6.String() + "]"
	}
	return EndpointCandidate{Address: net.JoinHostPort(host, strconv.Itoa(entry.Port))}
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
