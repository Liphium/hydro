package bridge

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const UDPTimeout = 15 * time.Second

// Create a new UDP bridge that binds on the origin and forwards to the target.
func NewUDPBridge(origin string, originPort uint, target string, targetPort uint) Bridge {
	return &udpBridge{
		OriginHost: origin,
		OriginPort: originPort,
		TargetHost: target,
		TargetPort: targetPort,
	}
}

type udpBridge struct {
	OriginHost string
	OriginPort uint
	TargetHost string
	TargetPort uint

	conn        net.PacketConn
	connections *sync.Map
	targetAddr  *net.UDPAddr
}

func (u *udpBridge) Start() error {
	// Resolve target address
	targetAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", u.TargetHost, u.TargetPort))
	if err != nil {
		return fmt.Errorf("couldn't resolve target address: %v", err)
	}
	u.targetAddr = targetAddr

	// Try to create the listener
	addr := fmt.Sprintf("%s:%d", u.OriginHost, u.OriginPort)
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return fmt.Errorf("couldn't create bridge: %v", err)
	}

	// Initialize the connection
	u.conn = conn
	u.connections = &sync.Map{}

	// Create the packet bridge
	go func() {
		buffer := make([]byte, 64*1024) // Larger buffer for UDP

		defer func() {
			conn.Close()
		}()

		for {
			// Read a packet from the UDP connection
			n, addr, err := conn.ReadFrom(buffer)
			if err != nil {
				logger.Printf("Couldn't read UDP packet: %v \n", err)
				return
			}

			// Handle the packet in a new goroutine
			go u.handlePacket(addr, buffer[:n])
		}
	}()

	return nil
}

// Helper function that handles a packet and potentially opens a new connection to redirect
func (u *udpBridge) handlePacket(sourceAddr net.Addr, packet []byte) {

	// Make sure a connection exists
	if _, ok := u.connections.Load(sourceAddr); !ok {

		// Create the connection and add to the map
		targetConn, err := net.DialUDP("udp", nil, u.targetAddr)
		if err != nil {
			logger.Printf("Couldn't connect to target: %v\n", err)
			return
		}

		u.connections.Store(sourceAddr, targetConn)

		// Listen for responses from the target and forward, also manage connection state
		go u.listenForResponse(sourceAddr, targetConn)
	}

	obj, ok := u.connections.Load(sourceAddr)
	if !ok {
		logger.Println("Connection for target has been deleted.")
		return
	}
	connection := obj.(*net.UDPConn)

	// Forward the packet
	_, err := io.Copy(connection, bytes.NewReader(packet))
	if err != nil {
		logger.Printf("Couldn't write packet to target: %v\n", err)
		return
	}
}

// Helper function that listens to packets from the connection and also manages its lifetime
func (u *udpBridge) listenForResponse(sourceAddr net.Addr, targetConn *net.UDPConn) {
	buffer := make([]byte, 64*1024)
	targetConn.SetReadBuffer(64 * 1024)
	defer func() {
		targetConn.Close()
		u.connections.Delete(sourceAddr)
	}()

	for {
		targetConn.SetReadDeadline(time.Now().Add(UDPTimeout))

		n, err := targetConn.Read(buffer)
		if err != nil {
			logger.Println("Couldn't read from UDP connection:", err)
			return
		}

		// Send response back to original source
		_, err = u.conn.WriteTo(buffer[:n], sourceAddr)
		if err != nil {
			logger.Printf("Couldn't write response back to source: %v\n", err)
			return
		}
	}
}

func (u *udpBridge) Close() error {
	if u.conn != nil {
		return u.conn.Close()
	}
	return nil
}

func (u *udpBridge) String() string {
	return fmt.Sprintf("%s: %s:%d -> %s:%d", "udp", u.OriginHost, u.OriginPort, u.TargetHost, u.TargetPort)
}
