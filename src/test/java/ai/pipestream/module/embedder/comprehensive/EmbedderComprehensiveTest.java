package ai.pipestream.module.embedder.comprehensive;

import ai.pipestream.common.util.ProcessingBuffer;
import ai.pipestream.common.util.ProcessingBufferFactory;
import ai.pipestream.data.module.*;
import ai.pipestream.data.util.proto.ProtobufTestDataHelper;
import ai.pipestream.data.v1.PipeDoc;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Comprehensive test for embedder module processing chunked documents with different embedding models.
 * Tests two configurations:
 * 1. nomic-ai/nomic-embed-text-v1.5 (768 dimensions)
 * 2. BAAI/bge-small-en-v1.5 (384 dimensions)
 */
@QuarkusTest
public class EmbedderComprehensiveTest {
    private static final Logger LOG = LoggerFactory.getLogger(EmbedderComprehensiveTest.class);

    @GrpcClient
    PipeStepProcessor embedderService;

    private ProtobufTestDataHelper testDataHelper;
    private List<PipeDoc> chunkedDocuments;

    @BeforeEach
    public void setup() {
        testDataHelper = new ProtobufTestDataHelper();
        chunkedDocuments = new ArrayList<>();
    }

    @Test
    public void testEmbedWithNomicModel() throws Exception {
        // Load chunker documents
        testDataHelper.getChunkerPipeDocuments().forEach(chunkedDocuments::add);
        LOG.info("Loaded {} chunked documents for embedding", chunkedDocuments.size());
        
        // Verify we have documents
        if (chunkedDocuments.isEmpty()) {
            LOG.error("No chunker documents were loaded! Check ProtobufTestDataHelper.");
            throw new AssertionError("No chunker documents were loaded");
        }
        
        LOG.info("\n=== Testing Model 1: nomic-ai/nomic-embed-text-v1.5 (768 dimensions) ===");
        
        Path outputDir = Paths.get("build/test-data/embedder/output-nomic");
        Files.createDirectories(outputDir);
        
        ProcessingBuffer<PipeDoc> outputBuffer = ProcessingBufferFactory.createBuffer(
                true, 1000, PipeDoc.class
        );
        
        // Configuration 1: Nomic embedding model (768 dimensions)
        String nomicConfigJson = """
                {
                    "model_name": "nomic-ai/nomic-embed-text-v1.5",
                    "model_dimension": 768,
                    "batch_size": 32,
                    "gpu_index": 0,
                    "result_set_name_template": "embeddings_%s_%s",
                    "field_mappings": [
                        {
                            "source_field": "chunks",
                            "embedding_field": "embedding",
                            "text_path": "text"
                        },
                        {
                            "source_field": "body",
                            "embedding_field": "document_embedding"
                        }
                    ]
                }
                """;

        int successCount = 0;
        int failCount = 0;
        
        // Process first 20 documents
        int docsToProcess = Math.min(20, chunkedDocuments.size());
        
        for (int i = 0; i < docsToProcess; i++) {
            PipeDoc doc = chunkedDocuments.get(i);
            
            try {
                ModuleProcessRequest request = createProcessRequest(doc, nomicConfigJson, "nomic");
                
                LOG.info("Processing document {}/{}: {} ({} chunks)", 
                        i + 1, docsToProcess, doc.getDocId(),
                        doc.hasSearchMetadata() && doc.getSearchMetadata().getSemanticResultsCount() > 0 ? doc.getSearchMetadata().getSemanticResults(0).getChunksCount() : 0);
                
                ModuleProcessResponse response = embedderService.processData(request)
                        .await().atMost(java.time.Duration.ofSeconds(60));

                if (response != null && response.getSuccess() && response.hasOutputDoc()) {
                    PipeDoc outputDoc = response.getOutputDoc();
                    outputBuffer.add(outputDoc);
                    successCount++;
                    
                    // Check embeddings were created
                    int embeddingCount = outputDoc.hasSearchMetadata() && outputDoc.getSearchMetadata().getSemanticResultsCount() > 0 ? 
                            outputDoc.getSearchMetadata().getSemanticResults(0).getChunksCount() : 0;
                    LOG.info("✓ Successfully created {} embeddings", embeddingCount);
                } else {
                    failCount++;
                    LOG.warn("✗ [EMBEDDER-NOMIC] Failed to embed document {}: {}", doc.getDocId(),
                            response != null ? response.getProcessorLogsList() : "null response");
                }
            } catch (Exception e) {
                failCount++;
                LOG.error("✗ [EMBEDDER-NOMIC] Error processing document {} (chunks: {}): {} - Exception Type: {} - Cause: {}", 
                    doc.getDocId(), 
                    doc.hasSearchMetadata() && doc.getSearchMetadata().getSemanticResultsCount() > 0 ? doc.getSearchMetadata().getSemanticResults(0).getChunksCount() : 0,
                    e.getMessage(), 
                    e.getClass().getSimpleName(),
                    e.getCause() != null ? e.getCause().getMessage() : "None");
                
                // Log the first few stack trace elements for debugging
                if (e.getStackTrace().length > 0) {
                    LOG.error("✗ [EMBEDDER-NOMIC] Stack trace origin: {} at line {}", 
                        e.getStackTrace()[0].getClassName() + "." + e.getStackTrace()[0].getMethodName(),
                        e.getStackTrace()[0].getLineNumber());
                }
            }
        }

        LOG.info("\n=== Nomic Model Summary ===");
        LOG.info("Processed: {}", successCount + failCount);
        LOG.info("Successful: {}", successCount);
        LOG.info("Failed: {}", failCount);
        
        // Save results
        outputBuffer.saveToDisk(outputDir, "embedder_nomic_output", 3);
        LOG.info("✓ Saved {} documents to {}", outputBuffer.size(), outputDir);
    }

    @Test
    public void testEmbedWithBGEModel() throws Exception {
        // Load chunker documents
        testDataHelper.getChunkerPipeDocuments().forEach(chunkedDocuments::add);
        LOG.info("Loaded {} chunked documents for embedding", chunkedDocuments.size());
        
        LOG.info("\n=== Testing Model 2: BAAI/bge-small-en-v1.5 (384 dimensions) ===");
        
        Path outputDir = Paths.get("build/test-data/embedder/output-bge");
        Files.createDirectories(outputDir);
        
        ProcessingBuffer<PipeDoc> outputBuffer = ProcessingBufferFactory.createBuffer(
                true, 1000, PipeDoc.class
        );
        
        // Configuration 2: BGE embedding model (384 dimensions)
        String bgeConfigJson = """
                {
                    "model_name": "BAAI/bge-small-en-v1.5",
                    "model_dimension": 384,
                    "batch_size": 32,
                    "gpu_index": 0,
                    "result_set_name_template": "embeddings_%s_%s",
                    "field_mappings": [
                        {
                            "source_field": "chunks",
                            "embedding_field": "embedding",
                            "text_path": "text"
                        },
                        {
                            "source_field": "body",
                            "embedding_field": "document_embedding"
                        }
                    ]
                }
                """;

        int successCount = 0;
        int failCount = 0;
        
        // Process first 20 documents
        int docsToProcess = Math.min(20, chunkedDocuments.size());
        
        for (int i = 0; i < docsToProcess; i++) {
            PipeDoc doc = chunkedDocuments.get(i);
            
            try {
                ModuleProcessRequest request = createProcessRequest(doc, bgeConfigJson, "bge");
                
                LOG.info("Processing document {}/{}: {} ({} chunks)", 
                        i + 1, docsToProcess, doc.getDocId(),
                        doc.hasSearchMetadata() && doc.getSearchMetadata().getSemanticResultsCount() > 0 ? doc.getSearchMetadata().getSemanticResults(0).getChunksCount() : 0);
                
                ModuleProcessResponse response = embedderService.processData(request)
                        .await().atMost(java.time.Duration.ofSeconds(60));

                if (response != null && response.getSuccess() && response.hasOutputDoc()) {
                    PipeDoc outputDoc = response.getOutputDoc();
                    outputBuffer.add(outputDoc);
                    successCount++;
                    
                    // Check embeddings were created
                    int embeddingCount = outputDoc.hasSearchMetadata() && outputDoc.getSearchMetadata().getSemanticResultsCount() > 0 ? 
                            outputDoc.getSearchMetadata().getSemanticResults(0).getChunksCount() : 0;
                    LOG.info("✓ Successfully created {} embeddings", embeddingCount);
                } else {
                    failCount++;
                    LOG.warn("✗ [EMBEDDER-BGE] Failed to embed document {}: {}", doc.getDocId(),
                            response != null ? response.getProcessorLogsList() : "null response");
                }
            } catch (Exception e) {
                failCount++;
                LOG.error("✗ [EMBEDDER-BGE] Error processing document {} (chunks: {}): {} - Exception Type: {} - Cause: {}", 
                    doc.getDocId(), 
                    doc.hasSearchMetadata() && doc.getSearchMetadata().getSemanticResultsCount() > 0 ? doc.getSearchMetadata().getSemanticResults(0).getChunksCount() : 0,
                    e.getMessage(), 
                    e.getClass().getSimpleName(),
                    e.getCause() != null ? e.getCause().getMessage() : "None");
                
                // Log the first few stack trace elements for debugging
                if (e.getStackTrace().length > 0) {
                    LOG.error("✗ [EMBEDDER-BGE] Stack trace origin: {} at line {}", 
                        e.getStackTrace()[0].getClassName() + "." + e.getStackTrace()[0].getMethodName(),
                        e.getStackTrace()[0].getLineNumber());
                }
            }
        }

        LOG.info("\n=== BGE Model Summary ===");
        LOG.info("Processed: {}", successCount + failCount);
        LOG.info("Successful: {}", successCount);
        LOG.info("Failed: {}", failCount);
        
        // Save results
        outputBuffer.saveToDisk(outputDir, "embedder_bge_output", 3);
        LOG.info("✓ Saved {} documents to {}", outputBuffer.size(), outputDir);
    }

    private ModuleProcessRequest createProcessRequest(PipeDoc doc, String configJson, String configName) throws Exception {
        ServiceMetadata metadata = ServiceMetadata.newBuilder()
                .setPipelineName("test-embedding-" + configName)
                .setPipeStepName("embedder")
                .setStreamId(UUID.randomUUID().toString())
                .build();

        Struct.Builder structBuilder = Struct.newBuilder();
        JsonFormat.parser().merge(configJson, structBuilder);
        
        ProcessConfiguration config = ProcessConfiguration.newBuilder()
                .setCustomJsonConfig(structBuilder.build())
                .build();

        return ModuleProcessRequest.newBuilder()
                .setDocument(doc)
                .setMetadata(metadata)
                .setConfig(config)
                .build();
    }
}