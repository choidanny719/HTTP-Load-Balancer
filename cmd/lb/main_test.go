package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/choidanny719/HTTP-Load-Balancer/internal/balancer"
)

func TestListenerFailuresReleaseProxyPort(t *testing.T) {
	for _, occupied := range []string{"proxy", "admin"} {
		t.Run(occupied, func(t *testing.T) {
			held, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			available, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := available.Addr().String()
			available.Close()
			proxyAddress, adminAddress := address, held.Addr().String()
			if occupied == "proxy" {
				proxyAddress, adminAddress = adminAddress, proxyAddress
			}
			b, err := balancer.New(balancer.Config{Backends: []string{"http://127.0.0.1:9001"}, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := serve(ctx, proxyAddress, adminAddress, b, time.Second); err == nil {
				t.Fatal("startup succeeded despite an occupied port")
			}
			listener, err := net.Listen("tcp", address)
			if err != nil {
				t.Fatalf("startup failure leaked a listener: %v", err)
			}
			listener.Close()
		})
	}
}
