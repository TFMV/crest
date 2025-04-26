package models

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// BatchInfo contains information about a batch of data read from a Flight server
type BatchInfo struct {
	// ViewName is the name of the view/table from which the batch was read
	ViewName string

	// Server is the address of the Flight server from which the batch was read
	Server string

	// RecordBatch is the Arrow RecordBatch containing the data
	RecordBatch arrow.Record

	// Timestamp is the time at which the batch was read
	Timestamp time.Time
}

// TableInfo contains information about a table
type TableInfo struct {
	// Namespace is the Iceberg namespace
	Namespace string

	// Name is the name of the table
	Name string

	// Schema is the Arrow schema of the table
	Schema *arrow.Schema
}

// BatchCommitInfo contains information about a committed batch
type BatchCommitInfo struct {
	// TableInfo contains information about the table
	TableInfo *TableInfo

	// FilePath is the path to the file containing the data
	FilePath string

	// NumRows is the number of rows in the batch
	NumRows int64

	// BatchTimestamp is the timestamp of when the batch was read
	BatchTimestamp time.Time

	// CommitTimestamp is the timestamp of when the batch was committed
	CommitTimestamp time.Time
}
