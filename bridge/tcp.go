package bridge

import (
	"fmt"
	"io"
	"log"
	"net"
)

// Create a new TCP bridge that binds on the origin and forwards to the target.
func NewTCPBridge(origin string, originPort uint, target string, targetPort uint) Bridge {
	return &tcpBridge{
		OriginHost: origin,
		OriginPort: originPort,
		TargetHost: target,
		TargetPort: targetPort,
	}
}

type tcpBridge struct {
	OriginHost string
	OriginPort uint
	TargetHost string
	TargetPort uint

	listener net.Listener
	done     chan struct{}
}

// Start starts a new goroutine that binds on the port and starts proxying requests
func (t *tcpBridge) Start() error {
	t.done = make(chan struct{})

	// Create the listener address
	addr := fmt.Sprintf("%s:%d", t.OriginHost, t.OriginPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	t.listener = listener

	logger.Printf("TCP bridge started: %s -> %s:%d", addr, t.TargetHost, t.TargetPort)

	go func() {

		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-t.done:
					// Listener was closed intentionally
					return
				default:
					logger.Printf("failed to accept connection: %v", err)
					continue
				}
			}

			// Handle each connection in a separate goroutine
			go t.handleConnection(conn)
		}
	}()

	return nil
}

func (t *tcpBridge) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	// Connect to the target
	targetAddr := net.JoinHostPort(t.TargetHost, fmt.Sprintf("%d", t.TargetPort))
	targetConn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		logger.Printf("failed to connect to target %s: %v", targetAddr, err)
		return
	}
	defer targetConn.Close()

	// Bidirectional copy
	done := make(chan struct{}, 2)

	// Client -> Target
	go func() {
		io.Copy(targetConn, clientConn)
		done <- struct{}{}
	}()

	// Target -> Client
	go func() {
		io.Copy(clientConn, targetConn)
		done <- struct{}{}
	}()

	// Wait for either direction to complete
	<-done
}

func (t *tcpBridge) Close() error {
	if t.listener != nil {
		close(t.done)
		t.listener.Close()
		log.Printf("TCP bridge stopped: %s:%d", t.OriginHost, t.OriginPort)
	}
	return nil
}

func (t *tcpBridge) String() string {
	return fmt.Sprintf("%s: %s:%d -> %s:%d", "tcp", t.OriginHost, t.OriginPort, t.TargetHost, t.TargetPort)
}
