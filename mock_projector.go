package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:4352")
	if err != nil {
		fmt.Println("Error starting mock projector:", err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("Mock Projector listening on 127.0.0.1:4352...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}
		
		fmt.Println("Gateway connected!")
		buffer := make([]byte, 1024)
		n, _ := conn.Read(buffer)
		
		fmt.Printf("Received raw bytes from Gateway: %s\n", string(buffer[:n]))
		
		// Simulate a PJLink response (Power = ON)
		response := "%1POWR=1\r"
		conn.Write([]byte(response))
		fmt.Println("Sent PJLink response:", response)
		
		conn.Close()
	}
}
