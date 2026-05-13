package main

import (
	"log"
	"os"
)

func main() {
	addr := ":8080"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	log.Printf("io-multiplexing HTTP server listening on %s", addr)
	err := NewEngine().OnPacket(newHTTPHandler()).Listen(addr)
	if err != nil {
		log.Fatal(err)
	}
}
