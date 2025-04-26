package ingestor

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/TFMV/crest/pkg/config"
	"github.com/TFMV/crest/pkg/models"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// FlightReader is responsible for connecting to RisingWave Flight servers
// and continuously reading Arrow RecordBatches from materialized views
type FlightReader struct {
	config      *config.Config
	clients     map[string]flight.Client
	clientsLock sync.RWMutex
	pool        memory.Allocator
}

// NewFlightReader creates a new FlightReader
func NewFlightReader(cfg *config.Config) (*FlightReader, error) {
	return &FlightReader{
		config:  cfg,
		clients: make(map[string]flight.Client),
		pool:    memory.NewGoAllocator(),
	}, nil
}

// Connect establishes connections to all configured Flight servers
func (fr *FlightReader) Connect(ctx context.Context) error {
	for _, server := range fr.config.Flight.Servers {
		if err := fr.connectToServer(ctx, server); err != nil {
			log.Printf("Failed to connect to Flight server %s: %v", server, err)
			continue
		}
		log.Printf("Connected to Flight server: %s", server)
	}

	if len(fr.clients) == 0 {
		return fmt.Errorf("failed to connect to any Flight servers")
	}

	return nil
}

// connectToServer connects to a single Flight server
func (fr *FlightReader) connectToServer(ctx context.Context, server string) error {
	// Create gRPC client options with insecure transport (can be replaced with TLS if needed)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// Create a Flight client for the server
	client, err := flight.NewClientWithMiddleware(server, nil, nil, opts...)
	if err != nil {
		return fmt.Errorf("failed to create Flight client: %w", err)
	}

	// Store the client in the map
	fr.clientsLock.Lock()
	defer fr.clientsLock.Unlock()
	fr.clients[server] = client

	return nil
}

// ListFlights lists available flights (views/tables) from a Flight server
func (fr *FlightReader) ListFlights(ctx context.Context, server string) ([]*flight.FlightInfo, error) {
	fr.clientsLock.RLock()
	client, ok := fr.clients[server]
	fr.clientsLock.RUnlock()

	if !ok {
		// If client doesn't exist, try to connect and get it
		if err := fr.connectToServer(ctx, server); err != nil {
			return nil, fmt.Errorf("no client available for server %s: %w", server, err)
		}

		fr.clientsLock.RLock()
		client = fr.clients[server]
		fr.clientsLock.RUnlock()
	}

	// Create a criteria object - nil means list everything
	criteria := &flight.Criteria{}

	// Get the flight listing
	flightListing, err := client.ListFlights(ctx, criteria)
	if err != nil {
		return nil, fmt.Errorf("failed to list flights: %w", err)
	}

	// Collect all flight infos
	var flightInfos []*flight.FlightInfo
	for {
		info, err := flightListing.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error receiving flight info: %w", err)
		}

		flightInfos = append(flightInfos, info)
	}

	return flightInfos, nil
}

// GetSchema fetches the schema for a particular view/table from the Flight server
func (fr *FlightReader) GetSchema(ctx context.Context, server, viewName string) (*arrow.Schema, error) {
	fr.clientsLock.RLock()
	client, ok := fr.clients[server]
	fr.clientsLock.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no client available for server %s", server)
	}

	// Create a FlightDescriptor for the view
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{viewName},
	}

	// Get FlightInfo
	info, err := client.GetFlightInfo(ctx, desc)
	if err != nil {
		return nil, fmt.Errorf("failed to get flight info: %w", err)
	}

	// Get schema from FlightInfo's Schema bytes
	schema, err := flight.DeserializeSchema(info.Schema, fr.pool)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize schema: %w", err)
	}

	return schema, nil
}

// ReadBatches reads RecordBatches from a Flight server for a specific view
// and sends them to the provided channel until the context is cancelled or an error occurs
func (fr *FlightReader) ReadBatches(ctx context.Context, server, viewName string, batchChan chan<- *models.BatchInfo) error {
	fr.clientsLock.RLock()
	client, ok := fr.clients[server]
	fr.clientsLock.RUnlock()

	if !ok {
		return fmt.Errorf("no client available for server %s", server)
	}

	// Create a FlightDescriptor for the view
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{viewName},
	}

	// Get FlightInfo
	info, err := client.GetFlightInfo(ctx, desc)
	if err != nil {
		return fmt.Errorf("failed to get flight info: %w", err)
	}

	// For each endpoint in the FlightInfo, read the data
	for _, endpoint := range info.Endpoint {
		if len(endpoint.Location) == 0 {
			continue
		}

		// For simplicity, we'll just use the first location
		ticket := endpoint.Ticket

		// Create a FlightDataStream for the endpoint
		stream, err := client.DoGet(ctx, ticket)
		if err != nil {
			return fmt.Errorf("failed to do get: %w", err)
		}

		// Create a record reader from the stream
		reader, err := flight.NewRecordReader(stream)
		if err != nil {
			return fmt.Errorf("failed to create record reader: %w", err)
		}
		defer reader.Release()

		// Read batches from the stream and send them to the channel
		for reader.Next() {
			record := reader.Record()
			record.Retain() // Retain the record so it's not released when we move to the next one

			// Send the batch to the channel
			select {
			case <-ctx.Done():
				record.Release() // Release the record if context is done
				return ctx.Err()
			case batchChan <- &models.BatchInfo{
				ViewName:    viewName,
				Server:      server,
				RecordBatch: record,
				Timestamp:   time.Now(),
			}:
			}
		}

		// Check for errors after reading
		if err := reader.Err(); err != nil && err != io.EOF {
			return fmt.Errorf("error reading from stream: %w", err)
		}
	}

	return nil
}

// Close closes all Flight client connections
func (fr *FlightReader) Close() error {
	fr.clientsLock.Lock()
	defer fr.clientsLock.Unlock()

	var errs []error
	for server, client := range fr.clients {
		// Clean up resources by calling Close on the client
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing client for %s: %w", server, err))
		}

		// Remove from clients map
		delete(fr.clients, server)
	}

	// Clear the clients map
	fr.clients = make(map[string]flight.Client)

	if len(errs) > 0 {
		// For simplicity, just return the first error
		return errs[0]
	}
	return nil
}
