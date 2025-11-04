# Embedder Module

## Overview
The Embedder Module is a document processing service that generates vector embeddings for text content. It uses Deep Java Library (DJL) with PyTorch to create high-quality embeddings for document chunks and fields, enabling semantic search and similarity matching in the Rokkon pipeline.

## Features
- Generates vector embeddings for document chunks
- Supports multiple embedding models
- GPU acceleration (when available)
- Configurable batch processing for optimal performance
- Reactive processing with backpressure handling
- Comprehensive error handling
- Automatic model selection and fallback

## Supported Embedding Models
The Embedder Module supports various embedding models, including:
- ALL_MINILM_L6_V2 (default): Lightweight and fast, great for general-purpose sentence embeddings
- ALL_MPNET_BASE_V2: Higher accuracy than MiniLM, though a bit larger and slower
- ALL_DISTILROBERTA_V1: Based on DistilRoBERTa; good balance between performance and speed
- PARAPHRASE_MINILM_L3_V2: Even smaller and faster than L6 or L12 versions; ideal for low-latency scenarios
- PARAPHRASE_MULTILINGUAL_MINILM_L12_V2: Multilingual support (50+ languages) + small model size
- E5_SMALL_V2: Smaller sibling of e5-base-v2, good for retrieval tasks, especially with query-document use cases
- E5_LARGE_V2: Larger and more accurate than e5-base-v2, better embeddings at the cost of speed/memory
- MULTI_QA_MINILM_L6_COS_V1: Fine-tuned for semantic search and QA

## How It Works
The Embedder Module:
1. Receives documents with chunks through gRPC
2. Processes chunks in optimized batches
3. Generates vector embeddings using the configured model
4. Adds embeddings to the document chunks
5. Returns the document with embedded chunks

## Deployment

### Prerequisites
- JDK 21 or later
- Docker (for containerized deployment)
    - Access to Pipeline Engine and Consul services
- GPU with CUDA support (optional, for accelerated processing)

### Building the Module
The module can be built using the provided `docker-build.sh` script:

```bash
# Build in production mode
./docker-build.sh

# Build in development mode
./docker-build.sh dev
```

### Running the Module
After building, you can run the module using Docker:

```bash
# Production mode
docker run -i --rm -p 49093:49093 \
  -e ENGINE_HOST=engine \
  -e CONSUL_HOST=consul \
  rokkon/embedder-module:latest

# Development mode (uses host networking)
docker run -i --rm --network=host rokkon/embedder-module:dev
```

### Testing the Module
Use the provided `test-docker.sh` script to test the module:

```bash
# Test in development mode
./test-docker.sh

# Test in production mode
./test-docker.sh prod
```

## Configuration

### Environment Variables
- `MODULE_HOST`: Host address for the module (default: 0.0.0.0)
- `MODULE_PORT`: Port for the module (default: 9090)
- `ENGINE_HOST`: Host address for the Rokkon Engine (default: localhost)
- `ENGINE_PORT`: Port for the Rokkon Engine (default: 8081)
- `CONSUL_HOST`: Host address for Consul (default: empty)
- `CONSUL_PORT`: Port for Consul (default: -1)
- `HEALTH_CHECK`: Whether to perform health checks (default: true)
- `MAX_RETRIES`: Maximum number of registration retries (default: 3)

### Embedder Configuration Options
The embedder accepts the following configuration options in the `custom_json_config`:

- `embedding_models`: List of embedding models to use (default: ["ALL_MINILM_L6_V2"])
- `check_chunks`: Whether to check for and process chunks in the document (default: true)
- `check_document_fields`: Whether to check and process document fields if chunks are not present (default: true)
- `document_fields`: List of document fields to process if chunks are not present (default: ["body", "title"])
- `custom_field_mappings`: Custom field mappings for embedding specific fields (default: [])
- `process_keywords`: Whether to process keywords in the document (default: true)
- `keyword_ngram_sizes`: List of n-gram sizes to use for keywords (default: [1])
- `max_token_size`: Maximum token size for text to be embedded (default: 512)
- `max_batch_size`: Maximum batch size for GPU processing (default: 32)
- `backpressure_strategy`: Strategy for handling backpressure in reactive streams (default: "DROP_OLDEST")
- `log_prefix`: Prefix to add to log messages (default: "")
- `result_set_name_template`: Template for naming the result set (default: "%s_embeddings_%s")

## Integration
The Embedder Module integrates with the Rokkon pipeline through:

1. **gRPC Service**: Implements the `PipeStepProcessor` service defined in the Rokkon protobuf
2. **Registration**: Automatically registers with the Rokkon Engine on startup
3. **Health Checks**: Provides health status through the standard gRPC health check protocol

## Development
The module is built with Quarkus and uses:
- Deep Java Library (DJL) for ML model inference
- PyTorch as the ML engine
- Mutiny for reactive programming
- Micrometer and OpenTelemetry for observability

To contribute to the module, follow the standard Rokkon development workflow.

## Double Chunking Support
The Embedder Module supports processing documents that have been chunked multiple times. When a document has been processed by the chunker module twice, the embedder will generate embeddings for all chunks, resulting in 4 sets of embeddings (2 chunks after 2 embedding runs).