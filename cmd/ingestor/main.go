package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/TFMV/crest/pkg/config"
	"github.com/TFMV/crest/pkg/ingestor"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "configs/config.yaml", "Path to the configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, initiating shutdown", sig)
		cancel()
	}()

	// Create and start the ingestor
	ing, err := ingestor.NewIngestor(cfg)
	if err != nil {
		log.Fatalf("Failed to create ingestor: %v", err)
	}

	log.Printf("Starting ingestor with config: %+v", cfg)

	// Start the ingestor with the context for controlled shutdown
	err = ing.Start(ctx)
	if err != nil {
		log.Fatalf("Ingestor failed: %v", err)
	}

	// If we get here, the ingestor has shut down gracefully
	log.Println("Ingestor has shut down gracefully")
}
