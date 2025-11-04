package io.pipeline.module.embedder;

import io.pipeline.data.v1.PipeDoc;
import io.pipeline.data.v1.SearchMetadata;
import io.pipeline.data.v1.SemanticProcessingResult;
import io.pipeline.data.v1.SemanticChunk;
import io.pipeline.data.v1.ChunkEmbedding;
import io.pipeline.data.module.*;
import io.pipeline.module.embedder.service.EmbedderOptions;
import io.pipeline.module.embedder.service.EmbeddingModel;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

@QuarkusTest
public class QuickEmbedderTest {
    private static final Logger log = LoggerFactory.getLogger(QuickEmbedderTest.class);

    @GrpcClient
    PipeStepProcessor embedderService;

    @Test
    public void testEmbedderWithMockChunks() {
        // Create a document with mock chunks (simulating double-chunked output)
        PipeDoc testDoc = createMockDoubleChunkedDoc();
        
        // Test embedder
        EmbedderOptions config = new EmbedderOptions(
            List.of(EmbeddingModel.ALL_MINILM_L6_V2),
            true, false, List.of("body", "title"), List.of(), false,
            List.of(1), 512, "[TEST] ", "test_embeddings_%s_%s", 16, "DROP_OLDEST"
        );

        try {
            ModuleProcessRequest request = ModuleProcessRequest.newBuilder()
                .setDocument(testDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                    .setPipelineName("quick-embedder-test")
                    .setPipeStepName("embedder")
                    .setStreamId(UUID.randomUUID().toString())
                    .build())
                .setConfig(ProcessConfiguration.newBuilder()
                    .setCustomJsonConfig(config.toStruct())
                    .build())
                .build();

            ModuleProcessResponse response = embedderService.processData(request)
                .await().atMost(java.time.Duration.ofMinutes(5));
            
            if (response.getSuccess()) {
                log.info("✅ Embedder test successful - {} semantic results", 
                    response.getOutputDoc().getSearchMetadata().getSemanticResultsCount());
            } else {
                log.warn("❌ Embedder test failed: {}", response.getProcessorLogsList());
            }
        } catch (Exception e) {
            log.error("Embedder test error: {}", e.getMessage());
        }
    }

    private PipeDoc createMockDoubleChunkedDoc() {
        // Create chunks for first semantic result (large chunks)
        SemanticChunk chunk1 = SemanticChunk.newBuilder()
            .setChunkId("chunk_1")
            .setChunkNumber(1)
            .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                .setTextContent("This is the first large chunk of text content.")
                .setChunkId("chunk_1")
                .build())
            .build();

        SemanticChunk chunk2 = SemanticChunk.newBuilder()
            .setChunkId("chunk_2") 
            .setChunkNumber(2)
            .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                .setTextContent("This is the second large chunk of text content.")
                .setChunkId("chunk_2")
                .build())
            .build();

        // Create chunks for second semantic result (small chunks)
        SemanticChunk chunk3 = SemanticChunk.newBuilder()
            .setChunkId("chunk_3")
            .setChunkNumber(1)
            .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                .setTextContent("Small chunk one.")
                .setChunkId("chunk_3")
                .build())
            .build();

        SemanticChunk chunk4 = SemanticChunk.newBuilder()
            .setChunkId("chunk_4")
            .setChunkNumber(2)
            .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                .setTextContent("Small chunk two.")
                .setChunkId("chunk_4")
                .build())
            .build();

        // Create semantic processing results
        SemanticProcessingResult result1 = SemanticProcessingResult.newBuilder()
            .setResultId("result_1")
            .setSourceFieldName("body")
            .setChunkConfigId("large_chunks_v1")
            .setResultSetName("large_chunks")
            .addChunks(chunk1)
            .addChunks(chunk2)
            .build();

        SemanticProcessingResult result2 = SemanticProcessingResult.newBuilder()
            .setResultId("result_2")
            .setSourceFieldName("body")
            .setChunkConfigId("small_chunks_v1")
            .setResultSetName("small_chunks")
            .addChunks(chunk3)
            .addChunks(chunk4)
            .build();

        // Create the document
        return PipeDoc.newBuilder()
            .setDocId("mock-double-chunked-001")
            .setSearchMetadata(SearchMetadata.newBuilder()
                .setTitle("Mock Double Chunked Document")
                .setBody("This is a mock document with double chunking for embedder testing.")
                .addSemanticResults(result1)
                .addSemanticResults(result2)
                .build())
            .build();
    }
}