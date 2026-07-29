package main

import (
	"flag"
	"io"
	"log"
	"net"
)

func main() {
	port := flag.String("port", "6379", "TCP port to listen on")
	flag.Parse()

	// net.Listen binds the port and starts the kernel queueing inbound
	// connections for us. It does not accept any of them yet.
	ln, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen on :%s: %v", *port, err)
	}
	defer ln.Close()
	log.Printf("listening on :%s", *port)

	for {
		// Accept blocks until a client connects, then returns a conn
		// representing that one client. The listener stays open.
		conn, err := ln.Accept()
		if err != nil {
			// One failed accept should not kill the server -- log and
			// keep serving everyone else.
			log.Printf("accept: %v", err)
			continue
		}
		// One goroutine per client. This is the whole concurrency model
		// (ADR-001): handleConn may block freely without stalling others.
		go handleConn(conn)
	}
}

// handleConn currently just dumps whatever the client sends. No parsing yet --
// this exists so we can see real RESP bytes on the wire before writing code
// that interprets them.
func handleConn(conn net.Conn) {
	defer conn.Close()
	log.Printf("client connected: %s", conn.RemoteAddr())
	defer log.Printf("client gone: %s", conn.RemoteAddr())

	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			// io.EOF means the client closed cleanly. Anything else is a
			// real error worth logging.
			if err != io.EOF {
				log.Printf("read from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		// %q escapes the CRLFs so they're visible rather than wrapping lines.
		log.Printf("read %d bytes: %q", n, buf[:n])
	}
}
