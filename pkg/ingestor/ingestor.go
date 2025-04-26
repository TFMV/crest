package ingestor

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/TFMV/crest/pkg/config"
	"github.com/TFMV/crest/pkg/models"
)

// Ingestor is the main component that orchestrates the data flow from
// Flight servers to Iceberg tables
type Ingestor struct {
	config     *config.Config
	reader     *FlightReader
	writer     *BatchWriter
	committer  *IcebergCommitter
	batchChan  chan *models.BatchInfo
	commitChan chan *models.BatchCommitInfo
	wg         sync.WaitGroup
}

// NewIngestor creates a new Ingestor
func NewIngestor(cfg *config.Config) (*Ingestor, error) {
	// Create the Flight reader
	reader, err := NewFlightReader(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Flight reader: %w", err)
	}

	// Create the batch writer
	writer, err := NewBatchWriter(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create batch writer: %w", err)
	}

	// Create the Iceberg committer
	committer, err := NewIcebergCommitter(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Iceberg committer: %w", err)
	}

	return &Ingestor{
		config:     cfg,
		reader:     reader,
		writer:     writer,
		committer:  committer,
		batchChan:  make(chan *models.BatchInfo, 100),       // Buffer for batches
		commitChan: make(chan *models.BatchCommitInfo, 100), // Buffer for commits
	}, nil
}

// Start starts the ingestor, which will continuously read batches from Flight servers,
// write them to storage, and commit them to Iceberg tables
func (ing *Ingestor) Start(ctx context.Context) error {
	// Connect to Flight servers
	if err := ing.reader.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to Flight servers: %w", err)
	}

	// Start the batch processor
	ing.wg.Add(1)
	go ing.processBatches(ctx)

	// Start the commit processor
	ing.wg.Add(1)
	go ing.processCommits(ctx)

	// For each server, discover available views and start reader goroutines
	for _, server := range ing.config.Flight.Servers {
		// Discover available views from the Flight server
		views, err := ing.discoverViews(ctx, server)
		if err != nil {
			log.Printf("Warning: Failed to discover views from server %s: %v", server, err)

			// If discovery fails, use a default view as fallback
			log.Printf("Using default view for server %s", server)
			views = []string{"test_view"}
		}

		log.Printf("Found %d views on server %s", len(views), server)

		// Start a reader goroutine for each view
		for _, viewName := range views {
			ing.wg.Add(1)
			go func(server, viewName string) {
				defer ing.wg.Done()
				log.Printf("Starting reader for view %s on server %s", viewName, server)
				if err := ing.readBatches(ctx, server, viewName); err != nil {
					log.Printf("Error reading batches from %s/%s: %v", server, viewName, err)
				}
			}(server, viewName)
		}
	}

	// Wait for all goroutines to finish
	ing.wg.Wait()
	return nil
}

// discoverViews discovers available views from a Flight server
func (ing *Ingestor) discoverViews(ctx context.Context, server string) ([]string, error) {
	// Call the Flight reader's ListFlights method to discover available views
	flightInfos, err := ing.reader.ListFlights(ctx, server)
	if err != nil {
		return nil, fmt.Errorf("failed to list flights: %w", err)
	}

	// For now, extract view names using a simple approach
	// In a production system, we would parse the FlightInfo descriptor properly
	views := make([]string, 0, len(flightInfos))
	for i, _ := range flightInfos {
		// Use the Flight reader to get schema for each discovered flight
		// and extract view name from metadata or use a default naming scheme
		viewName := fmt.Sprintf("view_%d", i)
		views = append(views, viewName)
	}

	// If no views were found, return an error
	if len(views) == 0 {
		return nil, fmt.Errorf("no views found on server %s", server)
	}

	return views, nil
}

// readBatches continuously reads batches from a Flight server for a specific view
func (ing *Ingestor) readBatches(ctx context.Context, server, viewName string) error {
	log.Printf("Starting to read batches from %s/%s", server, viewName)

	// Create a ticker for batch reading
	ticker := time.NewTicker(500 * time.Millisecond) // Adjust as needed
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Read batches from the Flight server
			err := ing.reader.ReadBatches(ctx, server, viewName, ing.batchChan)
			if err != nil {
				log.Printf("Error reading batches from %s/%s: %v", server, viewName, err)
				// Backoff or retry logic could be implemented here
				time.Sleep(time.Second)
			}
		}
	}
}

// processBatches processes batches from the batch channel, writes them to storage,
// and sends them to the commit channel
func (ing *Ingestor) processBatches(ctx context.Context) {
	defer ing.wg.Done()
	log.Println("Starting batch processor")

	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-ing.batchChan:
			// Write the batch to storage
			commitInfo, err := ing.writer.WriteBatch(ctx, batch)
			if err != nil {
				log.Printf("Error writing batch: %v", err)
				continue
			}

			// Send the commit info to the commit channel
			select {
			case <-ctx.Done():
				return
			case ing.commitChan <- commitInfo:
				// Successfully sent commit info
			}
		}
	}
}

// processCommits processes commits from the commit channel and commits them to Iceberg tables
func (ing *Ingestor) processCommits(ctx context.Context) {
	defer ing.wg.Done()
	log.Println("Starting commit processor")

	for {
		select {
		case <-ctx.Done():
			return
		case commitInfo := <-ing.commitChan:
			// Commit the batch to the Iceberg table
			if err := ing.committer.CommitBatch(ctx, commitInfo); err != nil {
				log.Printf("Error committing batch: %v", err)
				continue
			}

			log.Printf("Successfully committed batch for table %s.%s: %d rows",
				commitInfo.TableInfo.Namespace, commitInfo.TableInfo.Name, commitInfo.NumRows)
		}
	}
}

// Close closes the ingestor and any resources it holds
func (ing *Ingestor) Close() error {
	// Close the Flight reader
	if err := ing.reader.Close(); err != nil {
		log.Printf("Error closing Flight reader: %v", err)
	}

	// Close the Iceberg committer
	if err := ing.committer.Close(); err != nil {
		log.Printf("Error closing Iceberg committer: %v", err)
	}

	// Close channels
	close(ing.batchChan)
	close(ing.commitChan)

	return nil
}
