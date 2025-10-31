# Loan Processing Engine

A distributed data processing system for analyzing HDMA (Home Mortgage Disclosure Act) loan data using SQL, HDFS, and gRPC.

## Overview

This project implements a fault-tolerant loan data processing pipeline that:
- Extracts loan application data from a MySQL database
- Stores data in a distributed HDFS cluster with configurable replication
- Provides gRPC APIs for querying and analyzing loan statistics
- Handles DataNode failures gracefully through data partitioning strategies

## Architecture

The system consists of 6 Docker containers working together:

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │ gRPC
       ▼
┌─────────────┐         ┌──────────────┐
│ gRPC Server │────────▶│ MySQL Server │
│   (Python)  │         │   (CS544)    │
└──────┬──────┘         └──────────────┘
       │
       │ WebHDFS API
       ▼
┌─────────────────────────────────┐
│         HDFS Cluster            │
│  ┌──────────┐  ┌──────────────┐│
│  │ NameNode │  │  DataNodes   ││
│  │  (boss)  │  │  (3 nodes)   ││
│  └──────────┘  └──────────────┘│
└─────────────────────────────────┘
```

### Components

- **gRPC Server**: Orchestrates data operations between MySQL and HDFS
- **MySQL Server**: Stores HDMA loan application data (426,716 rows after filtering)
- **HDFS Cluster**: 
  - 1 NameNode for metadata management
  - 3 DataNodes for distributed storage
- **Client**: Command-line interface for invoking gRPC operations

## Features

### 1. Data Pipeline (`DbToHdfs`)
- Extracts loan data from MySQL with SQL joins
- Filters loans between $30,000 and $800,000
- Uploads to HDFS as Parquet format with 2x replication
- Uses 1MB block size for efficient distributed processing

### 2. Block Location Analysis (`BlockLocations`)
- Queries HDFS via WebHDFS API
- Shows distribution of data blocks across DataNodes
- Useful for understanding data locality and replication status

### 3. Loan Analysis (`CalcAvgLoan`)
- Calculates average loan amounts by county
- **Performance Optimization**: Creates county-specific partition files
- **Smart Caching**: Reuses partitions for repeated queries (2-3x faster)
- **Fault Tolerance**: Automatically recreates lost partitions from main dataset

## Prerequisites

- Docker and Docker Compose
- 8GB+ available RAM
- ~2GB disk space

## Quick Start

### 1. Build Docker Images

```bash
export PROJECT=p4

docker build . -f Dockerfile.hdfs -t p4-hdfs
docker build . -f Dockerfile.namenode -t p4-nn
docker build . -f Dockerfile.datanode -t p4-dn
docker build . -f Dockerfile.mysql -t p4-mysql
docker build . -f Dockerfile.server -t p4-server
```

### 2. Start the System

```bash
docker compose up -d
```

Wait ~30 seconds for MySQL to initialize the database.

### 3. Load Data into HDFS

```bash
docker exec p4-server-1 python3 /client.py DbToHdfs
```

Expected output: `Successfully uploaded data to HDFS`

### 4. Verify Data Upload

```bash
docker exec p4-nn-1 hdfs dfs -du -h /
```

Expected output:
```
14.4 M   28.9 M  hdfs://nn:9000/hdma-wi-2021.parquet
```

## Usage

### Check Block Distribution

```bash
docker exec p4-server-1 python3 /client.py BlockLocations -f /hdma-wi-2021.parquet
```

Example output:
```
{'7eb74ce67e75': 15, 'f7747b42d254': 7, '39750756065d': 8}
```

### Calculate Average Loan Amount

```bash
docker exec p4-server-1 python3 /client.py CalcAvgLoan -c 55001
```

Example output:
```
245000
create
```

The second line indicates the data source:
- `create`: First time query, partition created from main dataset
- `reuse`: Subsequent query, using cached partition (~2x faster)
- `recreate`: Partition was lost, recreated from main dataset

### Available County Codes

- `55001` - Adams County, WI
- `55003` - Ashland County, WI
- `55027` - Dodge County, WI
- `55059` - Kenosha County, WI
- `55133` - Waukesha County, WI

## Performance Analysis

The project includes automated performance testing:

```bash
docker build -f Dockerfile.analyzer -t p4-analyzer .

docker run --rm \
  --network=p4_default \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd)/outputs:/app/outputs" \
  p4-analyzer
```

This generates:
- `outputs/performance_results.csv` - Timing data
- `outputs/performance_analysis.png` - Visualization

**Results**: Partitioned queries are ~2-3x faster due to:
- Reduced I/O (smaller files)
- No filtering overhead
- Better data locality

## Fault Tolerance

The system handles single DataNode failures:

### Main Dataset (2x replication)
- Automatically handles failures via HDFS replication
- No manual intervention required

### Partition Files (1x replication)
- May become temporarily unavailable
- System automatically detects missing data
- Recreates partition from main dataset
- Returns `source="recreate"` to indicate recovery

### Testing Fault Tolerance

```bash
# Kill a DataNode
docker kill p4-dn-1

# Verify reduced live nodes
docker exec p4-nn-1 hdfs dfsadmin -fs hdfs://nn:9000 -report

# Test recovery
docker exec p4-server-1 python3 /client.py CalcAvgLoan -c 55001
```

## Project Structure

```
.
├── docker-compose.yml          # Container orchestration
├── Dockerfile.hdfs             # Base HDFS image
├── Dockerfile.namenode         # HDFS NameNode
├── Dockerfile.datanode         # HDFS DataNode
├── Dockerfile.mysql            # MySQL database
├── Dockerfile.server           # gRPC server
├── Dockerfile.analyzer         # Performance testing
├── lender.proto                # gRPC service definition
├── server.py                   # gRPC server implementation
├── client.py                   # CLI client
├── performance_analyzer.py     # Automated benchmarking
└── outputs/
    ├── performance_results.csv
    └── performance_analysis.png
```

## Technical Details

### Technologies Used
- **Python 3.10**: Core application logic
- **gRPC**: High-performance RPC framework
- **MySQL 8.4**: Relational database
- **Hadoop 3.3.6**: Distributed file system
- **PyArrow 17.0**: Parquet file processing
- **Pandas**: Data manipulation
- **Matplotlib**: Performance visualization

### Data Flow

1. **Extract**: SQL join between `applications` and `loan_types` tables
2. **Transform**: Filter loans by amount, convert to DataFrame
3. **Load**: Write Parquet files to HDFS with configured replication
4. **Query**: Read and filter Parquet files using PyArrow
5. **Optimize**: Cache filtered results as partition files

### HDFS Configuration
- **NameNode Port**: 9000 (HDFS protocol), 9870 (WebHDFS)
- **Block Size**: 1MB (configurable)
- **Replication**: 2x for main data, 1x for partitions
- **Format**: Parquet with Snappy compression

## Troubleshooting

### Container won't start
```bash
# Check logs
docker logs p4-server-1 -f

# Restart services
docker compose down
docker compose up -d
```

### MySQL connection refused
Wait 30-60 seconds after `docker compose up` for database initialization.

### Low disk space
```bash
docker system prune --volumes -f
```

### HDFS issues
```bash
# Check HDFS status
docker exec p4-nn-1 hdfs dfsadmin -fs hdfs://nn:9000 -report

# View NameNode logs
docker logs p4-nn-1
```

## Cleanup

```bash
docker compose down
docker system prune --volumes -f
```

## Development

### Rebuild After Code Changes

```bash
# Quick rebuild script
docker build . -f Dockerfile.server -t p4-server
docker compose down
docker compose up -d
```

### View Real-time Logs

```bash
docker logs p4-server-1 -f
```

## License

Educational project for CS544 - Data Systems course.

## Acknowledgments

Built using course materials from University of Wisconsin-Madison CS544.
