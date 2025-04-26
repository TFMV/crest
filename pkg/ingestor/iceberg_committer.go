package ingestor

import (
	"context"
	"fmt"
	"log"

	"github.com/TFMV/crest/pkg/config"
	"github.com/TFMV/crest/pkg/models"
	"github.com/TFMV/crest/pkg/utils"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
)

// IcebergCommitter is responsible for managing Iceberg tables and committing data
type IcebergCommitter struct {
	config  *config.Config
	catalog catalog.Catalog
	tables  map[string]*table.Table
}

// NewIcebergCommitter creates a new IcebergCommitter
func NewIcebergCommitter(cfg *config.Config) (*IcebergCommitter, error) {
	// Create REST catalog client
	restCatalog, err := createRESTCatalog(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST catalog: %w", err)
	}

	return &IcebergCommitter{
		config:  cfg,
		catalog: restCatalog,
		tables:  make(map[string]*table.Table),
	}, nil
}

// createRESTCatalog creates a REST catalog client
func createRESTCatalog(cfg *config.Config) (catalog.Catalog, error) {
	// Create catalog options for REST catalog
	catName := "crest-catalog"

	// Create a REST catalog using the direct API
	restCatalog, err := rest.NewCatalog(context.Background(), catName, cfg.Catalog.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST catalog: %w", err)
	}

	return restCatalog, nil
}

// GetOrCreateTable gets or creates an Iceberg table for a given table info
func (ic *IcebergCommitter) GetOrCreateTable(ctx context.Context, tableInfo *models.TableInfo) (*table.Table, error) {
	// Check if the table exists in the cache
	tableName := fmt.Sprintf("%s.%s", tableInfo.Namespace, tableInfo.Name)
	if tbl, ok := ic.tables[tableName]; ok {
		return tbl, nil
	}

	// Create identifier from namespace and name
	identifier := table.Identifier{tableInfo.Namespace, tableInfo.Name}

	// Try to load the table from the catalog
	tbl, err := ic.catalog.LoadTable(ctx, identifier, nil)
	if err == nil {
		// Table exists, cache and return it
		ic.tables[tableName] = tbl
		return tbl, nil
	}

	// Create the table if it doesn't exist
	tbl, err = ic.createTable(ctx, tableInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Cache the table and return
	ic.tables[tableName] = tbl
	return tbl, nil
}

// createTable creates a new Iceberg table
func (ic *IcebergCommitter) createTable(ctx context.Context, tableInfo *models.TableInfo) (*table.Table, error) {
	// Create namespace if it doesn't exist
	namespaceIdent := table.Identifier{tableInfo.Namespace}

	// Try to create the namespace (it's ok if it already exists)
	err := ic.catalog.CreateNamespace(ctx, namespaceIdent, nil)
	if err != nil {
		// Check if error is because namespace already exists, which is fine
		// Otherwise, return the error
		log.Printf("Note: namespace creation returned: %v (might already exist)", err)
	}

	log.Printf("Using namespace: %s", tableInfo.Namespace)

	// Convert Arrow schema to Iceberg schema
	icebergSchema, err := utils.ArrowSchemaToIcebergSchema(tableInfo.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert Arrow schema to Iceberg schema: %w", err)
	}

	// Create table properties
	identifier := table.Identifier{tableInfo.Namespace, tableInfo.Name}
	props := iceberg.Properties{
		"write.format.default": "parquet",
	}

	// Create the table with options
	tbl, err := ic.catalog.CreateTable(ctx, identifier, icebergSchema,
		catalog.WithProperties(props))
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	log.Printf("Created table: %s.%s", tableInfo.Namespace, tableInfo.Name)
	return tbl, nil
}

// CommitBatch commits a batch to an Iceberg table
func (ic *IcebergCommitter) CommitBatch(ctx context.Context, commitInfo *models.BatchCommitInfo) error {
	// Get or create the table
	tbl, err := ic.GetOrCreateTable(ctx, commitInfo.TableInfo)
	if err != nil {
		return fmt.Errorf("failed to get or create table: %w", err)
	}

	// Start a new transaction
	txn := tbl.NewTransaction()

	// Add the data file
	err = txn.AddFiles([]string{commitInfo.FilePath}, nil, false)
	if err != nil {
		return fmt.Errorf("failed to add file to transaction: %w", err)
	}

	// Commit the transaction
	_, err = txn.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Committed batch for table %s.%s: %d rows",
		commitInfo.TableInfo.Namespace, commitInfo.TableInfo.Name, commitInfo.NumRows)
	return nil
}

// Close closes the IcebergCommitter and any resources it holds
func (ic *IcebergCommitter) Close() error {
	// Currently no resources to close
	return nil
}
