package client

import (
	"context"
	"net"
	neturl "net/url"

	"golang.org/x/net/proxy"
)

// preparedSOCKSConnection supplies the already-configured first hop to the standard SOCKS handshake implementation.
type preparedSOCKSConnection struct {
	net.Conn
}

// Dial returns the prepared first hop without opening another socket.
func (c preparedSOCKSConnection) Dial(string, string) (net.Conn, error) {
	return c.Conn, nil
}

// DialContext returns the prepared first hop unless preparation has been canceled.
func (c preparedSOCKSConnection) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.Conn, nil
}

// prepareSOCKS bounds one SOCKS5 or SOCKS5H tunnel independently of TLS and HTTP admission.
func (c *Client) prepareSOCKS(ctx context.Context, connection net.Conn, proxyURL *neturl.URL,
	targetAddress string) (net.Conn, error) {
	dialer, err := proxy.FromURL(proxyURL, preparedSOCKSConnection{Conn: connection})
	if err != nil {
		return nil, err
	}
	tunnelContext, cancel := context.WithTimeout(ctx, c.config.HandshakeTimeout)
	defer cancel()
	return dialer.(proxy.ContextDialer).DialContext(tunnelContext, "tcp", targetAddress)
}
