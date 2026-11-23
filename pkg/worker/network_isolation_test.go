package worker

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestContainerIsolationRules(t *testing.T) {
	endpoints := []isolationEndpoint{{"10.100.0.10", "udp", 53}, {"fd00:10::a", "udp", 53}, {"10.0.4.20", "tcp", 443}}
	drop := func(network string) []string { return []string{"-d", network, "-j", "DROP"} }

	allow, deny := containerIsolationRules(false, false, nil, endpoints)
	require.Equal(t, [][]string{
		{"-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "RETURN"},
		{"-d", "10.100.0.10", "-p", "udp", "--dport", "53", "-j", "RETURN"},
		{"-d", "10.0.4.20", "-p", "tcp", "--dport", "443", "-j", "RETURN"},
	}, allow)
	require.Equal(t, []string{"-m", "addrtype", "--dst-type", "LOCAL", "-j", "DROP"}, deny[0])
	for _, network := range []string{"169.254.0.0/16", "192.168.0.0/20", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10"} {
		require.Contains(t, deny, drop(network))
	}
	require.NotContains(t, deny, drop("fc00::/7"))

	_, deny = containerIsolationRules(false, true, nil, endpoints)
	require.Contains(t, deny, drop("169.254.0.0/16"))
	require.Contains(t, deny, drop("192.168.0.0/20"))
	require.NotContains(t, deny, drop("10.0.0.0/8"))

	allow, deny = containerIsolationRules(true, false, []string{"2600:1f18:37a4:c00::/56"}, endpoints)
	require.Contains(t, allow, []string{"-p", "ipv6-icmp", "--icmpv6-type", "135", "-j", "RETURN"})
	require.Contains(t, allow, []string{"-d", "fd00:10::a", "-p", "udp", "--dport", "53", "-j", "RETURN"})
	require.NotContains(t, allow, []string{"-d", "10.100.0.10", "-p", "udp", "--dport", "53", "-j", "RETURN"})
	for _, network := range []string{"fe80::/10", "fd00:abcd::/64", "fd00:ec2::254/128", "fc00::/7", "64:ff9b::/96", "2600:1f18:37a4:c00::/56"} {
		require.Contains(t, deny, drop(network))
	}
	require.NotContains(t, deny, drop("10.0.0.0/8"))
}

func TestIsolateContainersFollowsMovedEndpoints(t *testing.T) {
	if !resolvConfHasUsableNameserver(hostResolvConfPath) {
		t.Skip("isolation reads the host's nameservers")
	}
	for _, name := range []string{types.ContainerGatewayGRPCHostEnv, types.ContainerGatewayGRPCPortEnv, types.ContainerGatewayHTTPHostEnv, types.ContainerGatewayHTTPPortEnv} {
		t.Setenv(name, "")
	}
	gateway := isolationEndpoint{"10.0.4.20", "tcp", 443}
	moved := isolationEndpoint{"10.0.4.21", "tcp", 443}
	manager := &ContainerNetworkManager{}
	manager.config.Worker.UseHostResolvConf = true
	manager.config.GatewayService.GRPC.ExternalHost = gateway.address
	manager.config.GatewayService.GRPC.ExternalPort = gateway.port
	require.NoError(t, manager.isolateContainers())
	require.Contains(t, manager.isolatedEndpoints, gateway)

	// A refresh with nothing new must not wait on container setup's iptables lock.
	manager.iptablesMu.Lock()
	refreshed := make(chan error, 1)
	go func() { refreshed <- manager.isolateContainers() }()
	select {
	case err := <-refreshed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("refresh without new endpoints waited for the iptables lock")
	}
	manager.iptablesMu.Unlock()

	manager.config.GatewayService.GRPC.ExternalHost = moved.address
	require.NoError(t, manager.isolateContainers())
	require.Contains(t, manager.isolatedEndpoints, gateway)
	require.Contains(t, manager.isolatedEndpoints, moved)
}

func TestBuildNetworkEnter(t *testing.T) {
	nsenter := []string{"nsenter", "--net=/var/run/netns/build", "--"}
	network := buildNetwork{netnsPath: "/var/run/netns/build"}
	require.Equal(t, nsenter, network.enter("pull"), "the worker's resolv.conf works in the network")

	network.resolvConf = workerResolvConfPath
	withResolvConf := append(nsenter, "unshare", "--mount", "--propagation", "private", "--", "sh", "-c", bindResolvConf, workerResolvConfPath)
	for _, sub := range []string{"pull", "from", "bud", "run", "add", "push"} {
		require.Equal(t, withResolvConf, network.enter(sub), sub)
	}
	for _, sub := range []string{"mount", "umount"} {
		require.Equal(t, nsenter, network.enter(sub), "the worker reads what %s mounts", sub)
	}
}

func TestBuildResolvConf(t *testing.T) {
	hostResolv := filepath.Join(t.TempDir(), "resolv.conf")
	require.Equal(t, workerResolvConfPath, buildResolvConf(hostResolv), "no resolv.conf")

	for _, tc := range []struct {
		hostResolv string
		want       string
	}{
		{"nameserver 127.0.0.53\noptions edns0 trust-ad\nsearch .\n", workerResolvConfPath},
		{"nameserver ::1\n", workerResolvConfPath},
		{"nameserver 10.43.0.10\n", ""},
		{"nameserver 127.0.0.53\nnameserver 10.0.0.2\n", ""},
	} {
		require.NoError(t, os.WriteFile(hostResolv, []byte(tc.hostResolv), 0o644))
		require.Equal(t, tc.want, buildResolvConf(hostResolv), tc.hostResolv)
	}
}

func TestHostNetworks(t *testing.T) {
	var addresses []net.Addr
	for _, cidr := range []string{"10.100.17.33/18", "2600:1f18:37a4:c02::a0a9/128", "2600:1f18:37a4:c03::1/64", "fd00:abcd::1/64", "fe80::1/64", "::1/128"} {
		ip, network, err := net.ParseCIDR(cidr)
		require.NoError(t, err)
		network.IP = ip
		addresses = append(addresses, network)
	}
	require.Equal(t, []string{"2600:1f18:37a4:c00::/56"}, hostNetworks(addresses))
}

func TestEndpointHostPort(t *testing.T) {
	for _, tc := range []struct {
		target string
		host   string
		port   int
	}{
		{"registry.localhost:5000", "registry.localhost", 5000},
		{"123456789012.dkr.ecr.us-east-1.amazonaws.com/beta9-images", "123456789012.dkr.ecr.us-east-1.amazonaws.com", 443},
		{"http://apt-cache.internal:3142", "apt-cache.internal", 3142},
		{"http://mirror.internal/ubuntu", "mirror.internal", 80},
		{"[fd00::5]:8443", "fd00::5", 8443},
		{"", "", 0},
	} {
		host, port := endpointHostPort(tc.target)
		require.Equal(t, tc.host, host, tc.target)
		require.Equal(t, tc.port, port, tc.target)
	}
}
