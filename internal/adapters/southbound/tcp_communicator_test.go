package southbound

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/team/edge-gateway/internal/core/domain"
)

func TestTCPCommunicator_SendCommand_PJLink(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		if _, err := conn.Write([]byte("PJLINK 0\r")); err != nil {
			return
		}

		buf := make([]byte, 64)
		n, err := conn.Read(buf)
		if err != nil {
			return
		}

		switch string(buf[:n]) {
		case "%1POWR ?\r":
			_, _ = conn.Write([]byte("%1POWR=1\r"))
		case "%1LAMP ?\r":
			_, _ = conn.Write([]byte("%1LAMP=105 1\r"))
		default:
			_, _ = conn.Write([]byte("%1ERR=ERR1\r"))
		}
	}()

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split host port failed: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port failed: %v", err)
	}

	comm := NewTCPCommunicator(2000)
	dev := &domain.Device{
		ID:       "Projector01",
		IP:       "127.0.0.1",
		Port:     port,
		Protocol: domain.ProtocolPJLink,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := comm.SendCommand(ctx, dev, []byte("%1POWR ?\r"))
	if err != nil {
		t.Fatalf("SendCommand failed: %v", err)
	}
	if string(resp) != "%1POWR=1\r" {
		t.Fatalf("got response %q, want %q", resp, "%1POWR=1\r")
	}
}
