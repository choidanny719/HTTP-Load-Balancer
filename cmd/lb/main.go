package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
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
	admin := flag.String("admin", "127.0.0.1:9090", "health and metrics listen address")
	backends := flag.String("backends", "", "comma-separated backend origins")
	timeout := flag.Duration("timeout", 5*time.Second, "request deadline")
	algorithm := flag.String("algorithm", "round-robin", "round-robin or least-connections")
	threshold := flag.Int("failure-threshold", 3, "consecutive failures before opening a circuit")
	cooldown := flag.Duration("cooldown", 10*time.Second, "delay before a recovery probe")
	rate := flag.Float64("rate", 20, "requests per second per client IP; 0 disables limiting")
	burst := flag.Int("burst", 40, "requests allowed in a burst")
	flag.Parse()
	if *threshold < 1 || *burst < 1 || *cooldown <= 0 || flag.NArg() != 0 {
		slog.Error("configuration", "error", "threshold, burst and cooldown must be positive; positional arguments are not supported")
		os.Exit(1)
	}
	b, err := balancer.New(balancer.Config{
		Backends: strings.Split(*backends, ","), Timeout: *timeout, Algorithm: *algorithm,
		FailureThreshold: *threshold, Cooldown: *cooldown,
		Rate: *rate, Burst: *burst,
	})
	if err != nil {
		slog.Error("configuration", "error", err)
		os.Exit(1)
	}
	defer b.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serve(ctx, *listen, *admin, b, *timeout); err != nil {
		slog.Error("server", "error", err)
		os.Exit(1)
	}
}

func serve(ctx context.Context, proxyAddress, adminAddress string, b *balancer.Balancer, timeout time.Duration) error {
	proxyListener, err := net.Listen("tcp", proxyAddress)
	if err != nil {
		return err
	}
	defer proxyListener.Close()
	adminListener, err := net.Listen("tcp", adminAddress)
	if err != nil {
		return err
	}
	defer adminListener.Close()
	proxy := &http.Server{
		Handler: b, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: timeout, WriteTimeout: timeout + time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 32 << 10,
	}
	admin := &http.Server{
		Handler: b.AdminHandler(), ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10,
	}
	done := make(chan error, 2)
	go func() { done <- proxy.Serve(proxyListener) }()
	go func() { done <- admin.Serve(adminListener) }()
	slog.Info("listening", "proxy", proxyListener.Addr(), "admin", adminListener.Addr())
	select {
	case err = <-done:
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, server := range []*http.Server{proxy, admin} {
		if shutdownErr := server.Shutdown(shutdown); shutdownErr != nil {
			server.Close()
		}
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
