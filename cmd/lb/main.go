package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/choidanny719/HTTP-Load-Balancer/internal/balancer"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "proxy listen address")
	backends := flag.String("backends", "", "comma-separated backend origins")
	timeout := flag.Duration("timeout", 5*time.Second, "request deadline")
	flag.Parse()
	b, err := balancer.New(balancer.Config{Backends: strings.Split(*backends, ","), Timeout: *timeout})
	if err != nil {
		slog.Error("configuration", "error", err)
		os.Exit(1)
	}
	defer b.Close()
	server := &http.Server{
		Addr: *listen, Handler: b, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: *timeout, WriteTimeout: *timeout + time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 32 << 10,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	slog.Info("listening", "address", *listen)
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			server.Close()
		}
	}
}
