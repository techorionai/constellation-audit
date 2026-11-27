package testserver

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
)

// TCPProxy forwards TCP connections to a target. Stop closes the listener and
// all open connections, so the target looks stopped. Start listens again on the
// same address. A test uses it to stop and start a database that has a fixed
// address for the service.
type TCPProxy struct {
	t      testing.TB
	target string
	addr   string

	mu    sync.Mutex
	l     net.Listener
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// NewTCPProxy starts a proxy to target (host:port) on a free local port. It
// stops at the cleanup of the test.
func NewTCPProxy(t testing.TB, target string) *TCPProxy {
	t.Helper()
	p := &TCPProxy{t: t, target: target, conns: map[net.Conn]struct{}{}}
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("testserver: proxy listen: %v", err)
	}
	p.addr = l.Addr().String()
	p.l = l
	p.serve(l)
	t.Cleanup(p.Stop)
	return p
}

// Addr returns the address of the proxy.
func (p *TCPProxy) Addr() string { return p.addr }

func (p *TCPProxy) serve(l net.Listener) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			p.conns[c] = struct{}{}
			p.mu.Unlock()
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				p.forward(c)
			}()
		}
	}()
}

func (p *TCPProxy) forward(c net.Conn) {
	defer func() {
		_ = c.Close()
		p.mu.Lock()
		delete(p.conns, c)
		p.mu.Unlock()
	}()
	up, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.target)
	if err != nil {
		return
	}
	p.mu.Lock()
	p.conns[up] = struct{}{}
	p.mu.Unlock()
	defer func() {
		_ = up.Close()
		p.mu.Lock()
		delete(p.conns, up)
		p.mu.Unlock()
	}()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, c); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
	<-done
}

// Stop closes the listener and all connections. It is safe to call it more
// than one time.
func (p *TCPProxy) Stop() {
	p.mu.Lock()
	l := p.l
	p.l = nil
	conns := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		conns = append(conns, c)
	}
	p.mu.Unlock()
	if l != nil {
		_ = l.Close()
	}
	for _, c := range conns {
		_ = c.Close()
	}
	p.wg.Wait()
}

// Start listens again on the address of the proxy.
func (p *TCPProxy) Start() {
	p.t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.l != nil {
		return
	}
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", p.addr)
	if err != nil {
		p.t.Fatalf("testserver: proxy listen again: %v", err)
	}
	p.l = l
	p.serve(l)
}
