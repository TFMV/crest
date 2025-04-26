# Crest

Crest is a Go-based solution that integrates RisingWave (for streaming transformations and materialized views), Apache Arrow Flight (for high-speed transport), Apache Iceberg (for table storage), and Lakekeeper (for REST-based Iceberg catalog management).

## Overview

The Crest system provides continuous ingestion from RisingWave materialized views via Arrow Flight into Iceberg tables managed by Lakekeeper. This creates a powerful streaming-to-lakehouse pipeline that combines the benefits of real-time streaming with the reliability and governance of a lakehouse architecture.

## Architecture

The system consists of the following components:

1. **Flight Reader**: Connects to RisingWave Flight servers and continuously reads Arrow RecordBatches from materialized views.

2. **Batch Writer**: Writes batches to storage in Parquet (preferred) or Arrow IPC format.

3. **Iceberg Committer**: Integrates with Lakekeeper REST catalog to manage Iceberg tables, metadata, and transactions.

4. **Ingestor**: Orchestrates the data flow between the components, managing concurrency, batching, and error handling.

## Features

- Continuous ingestion from multiple Flight servers and materialized views
- Automatic schema detection and evolution handling
- Local filesystem support (with S3 support planned)
- Transactional commits to Iceberg tables
- Batching based on row count or time window

## Installation

To build and run Crest, you need Go 1.21 or later.

```bash
# Clone the repository
git clone https://github.com/TFMV/crest.git
cd crest

# Build the application
go build -o bin/ingestor ./cmd/ingestor
```

## Configuration

Crest is configured using a YAML file. A sample configuration is provided in `configs/config.yaml`. The configuration includes:

- Flight server connections
- Iceberg and catalog settings
- Storage configuration
- Batching parameters
- Metrics collection options

## Usage

```bash
# Run the ingestor with the default configuration
./bin/ingestor

# Run with a custom configuration file
./bin/ingestor -config /path/to/config.yaml
```

## Requirements

- RisingWave Flight server(s) accessible over the network
- Lakekeeper REST catalog server running
- Local filesystem (or S3) for storage
- Go 1.21 or later for building

## License

[MIT](LICENSE)
