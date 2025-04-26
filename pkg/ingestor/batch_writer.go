package ingestor

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/TFMV/crest/pkg/config"
	"github.com/TFMV/crest/pkg/models"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"
)

// FileFormat represents the format in which data will be written
type FileFormat string

const (
	// ParquetFormat represents the Parquet file format
	ParquetFormat FileFormat = "parquet"

	// ArrowFormat represents the Arrow IPC file format
	ArrowFormat FileFormat = "arrow"
)

// BatchWriter is responsible for writing Arrow RecordBatches to storage
type BatchWriter struct {
	config     *config.Config
	format     FileFormat
	pool       memory.Allocator
	tableInfos map[string]*models.TableInfo
	mu         sync.RWMutex
}

// NewBatchWriter creates a new BatchWriter
func NewBatchWriter(cfg *config.Config) (*BatchWriter, error) {
	// Create base storage directory if it doesn't exist
	if cfg.Storage.Type == "local" {
		if err := os.MkdirAll(cfg.Storage.LocalPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create storage directory: %w", err)
		}
	}

	return &BatchWriter{
		config:     cfg,
		format:     ParquetFormat, // Default to Parquet
		pool:       memory.NewGoAllocator(),
		tableInfos: make(map[string]*models.TableInfo),
	}, nil
}

// GetOrCreateTableInfo gets or creates a TableInfo for a given view
func (bw *BatchWriter) GetOrCreateTableInfo(ctx context.Context, viewName string, schema *arrow.Schema) (*models.TableInfo, error) {
	bw.mu.RLock()
	tableInfo, ok := bw.tableInfos[viewName]
	bw.mu.RUnlock()

	if ok {
		return tableInfo, nil
	}

	// Create new TableInfo
	tableInfo = &models.TableInfo{
		Namespace: bw.config.Iceberg.DefaultNamespace,
		Name:      viewName,
		Schema:    schema,
	}

	// Store the TableInfo
	bw.mu.Lock()
	bw.tableInfos[viewName] = tableInfo
	bw.mu.Unlock()

	return tableInfo, nil
}

// WriteBatch writes a batch to storage and returns information about the write
func (bw *BatchWriter) WriteBatch(ctx context.Context, batch *models.BatchInfo) (*models.BatchCommitInfo, error) {
	// Get or create TableInfo
	tableInfo, err := bw.GetOrCreateTableInfo(ctx, batch.ViewName, batch.RecordBatch.Schema())
	if err != nil {
		return nil, fmt.Errorf("failed to get or create table info: %w", err)
	}

	// Generate a unique file path
	filePath, err := bw.generateFilePath(tableInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to generate file path: %w", err)
	}

	// Create the parent directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Write the batch to the file
	var numRows int64
	if bw.format == ParquetFormat {
		numRows, err = bw.writeParquet(batch.RecordBatch, filePath)
	} else {
		numRows, err = bw.writeArrow(batch.RecordBatch, filePath)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to write batch: %w", err)
	}

	// Return information about the commit
	return &models.BatchCommitInfo{
		TableInfo:       tableInfo,
		FilePath:        filePath,
		NumRows:         numRows,
		BatchTimestamp:  batch.Timestamp,
		CommitTimestamp: time.Now(),
	}, nil
}

// generateFilePath generates a unique file path for a batch
func (bw *BatchWriter) generateFilePath(tableInfo *models.TableInfo) (string, error) {
	// Generate a UUID for the file name
	fileUUID := uuid.New().String()

	var extension string
	if bw.format == ParquetFormat {
		extension = ".parquet"
	} else {
		extension = ".arrow"
	}

	// Create the file path
	var filePath string
	if bw.config.Storage.Type == "local" {
		// For local storage, use the configured local path
		filePath = filepath.Join(
			bw.config.Storage.LocalPath,
			tableInfo.Namespace,
			tableInfo.Name,
			fmt.Sprintf("%s%s", fileUUID, extension),
		)
	} else if bw.config.Storage.Type == "s3" {
		// For S3 storage, use the configured S3 bucket (not implemented yet)
		return "", fmt.Errorf("S3 storage not implemented yet")
	} else {
		return "", fmt.Errorf("unsupported storage type: %s", bw.config.Storage.Type)
	}

	return filePath, nil
}

// writeParquet writes a RecordBatch to a Parquet file
func (bw *BatchWriter) writeParquet(batch arrow.Record, filePath string) (int64, error) {
	// Create the file
	outputFile, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer outputFile.Close()

	// Create Parquet writer options
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithCreatedBy("Crest"),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(bw.pool))

	// Create the Parquet writer
	writer, err := pqarrow.NewFileWriter(
		batch.Schema(),
		outputFile,
		writerProps,
		arrowProps,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	// Write the batch to the file
	if err := writer.Write(batch); err != nil {
		return 0, fmt.Errorf("failed to write batch to Parquet file: %w", err)
	}

	// Close the writer
	if err := writer.Close(); err != nil {
		return 0, fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	log.Printf("Wrote %d rows to Parquet file: %s", batch.NumRows(), filePath)
	return int64(batch.NumRows()), nil
}

// writeArrow writes a RecordBatch to an Arrow IPC file
func (bw *BatchWriter) writeArrow(batch arrow.Record, filePath string) (int64, error) {
	// Create the file
	file, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create the Arrow writer
	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(batch.Schema()), ipc.WithAllocator(bw.pool))
	if err != nil {
		return 0, fmt.Errorf("failed to create Arrow writer: %w", err)
	}
	defer writer.Close()

	// Write the batch to the file
	if err := writer.Write(batch); err != nil {
		return 0, fmt.Errorf("failed to write batch to Arrow file: %w", err)
	}

	log.Printf("Wrote %d rows to Arrow file: %s", batch.NumRows(), filePath)
	return int64(batch.NumRows()), nil
}
