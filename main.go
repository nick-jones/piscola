package main

import (
	"log"
	"os"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/nick-jones/piscola/internal/gen-go/service"
	"github.com/nick-jones/piscola/internal/search"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	addr := "127.0.0.1:9090"
	if len(os.Args) == 2 {
		addr = os.Args[1]
	}

	transport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		return err
	}

	handler := search.NewService()
	processor := service.NewSearchProcessor(handler)
	server := thrift.NewTSimpleServer2(processor, transport)

	return server.Serve()
}