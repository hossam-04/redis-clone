// Command redis-clone is a Redis-compatible server built from scratch to
// learn systems programming. Real redis-cli connects to it unmodified.
//
// This file does as little as possible on purpose: parse flags, bind the
// port, wire the pieces together. Everything interesting lives under
// internal/ -- resp for the wire protocol, store for the data, server for
// the loop that joins them.
package main

import (
	"flag"
	"log"
	"net"

	"github.com/hossam-04/redis-clone/internal/server"
	"github.com/hossam-04/redis-clone/internal/store"
)

func main() {
	port := flag.String("port", "6379", "TCP port to listen on")
	flag.Parse()

	// net.Listen binds the port and has the kernel start queueing inbound
	// connections for us. It does not accept any of them yet.
	ln, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen on :%s: %v", *port, err)
	}
	defer ln.Close()
	log.Printf("listening on :%s", *port)

	log.Fatal(server.New(store.New()).Serve(ln))
}
