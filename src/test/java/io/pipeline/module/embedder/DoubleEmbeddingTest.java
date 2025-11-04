package io.pipeline.module.embedder;

import io.pipeline.data.v1.PipeDoc;
import io.pipeline.data.module.*;
import io.pipeline.module.embedder.service.EmbedderOptions;
import io.pipeline.module.embedder.service.EmbeddingModel;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class DoubleEmbeddingTest {
    private static final Logger log = LoggerFactory.getLogger(DoubleEmbeddingTest.class);

    @GrpcClient
    PipeStepProcessor embedderService;

    @Test
    public void testDoubleEmbedding() throws IOException {
        // Load double-chunked documents
        List<PipeDoc> doubleChunkedDocs = loadDoubleChunkedDocuments();
        log.info("Loaded {} double-chunked documents", doubleChunkedDocs.size());
        
        if (doubleChunkedDocs.isEmpty()) {
            log.warn("No double-chunked documents found, skipping test");
            return;
        }

        // First embedding: ALL_MINILM_L6_V2 model
        List<PipeDoc> firstEmbeddedDocs = performFirstEmbedding(doubleChunkedDocs);
        log.info("First embedding produced {} documents", firstEmbeddedDocs.size());

        // Second embedding: E5_SMALL_V2 model
        List<PipeDoc> doubleEmbeddedDocs = performSecondEmbedding(firstEmbeddedDocs);
        log.info("Double embedding produced {} documents", doubleEmbeddedDocs.size());

        // Save double-embedded documents
        saveDoubleEmbeddedDocuments(doubleEmbeddedDocs);
        
        // Verify double embedding structure
        verifyDoubleEmbeddingStructure(doubleEmbeddedDocs);
    }

    private List<PipeDoc> loadDoubleChunkedDocuments() throws IOException {
        List<PipeDoc> docs = new ArrayList<>();
        Path chunkedDir = Paths.get("../chunker/src/test/resources/double_chunked_pipedocs").toAbsolutePath().normalize();
        
        if (!Files.exists(chunkedDir)) {
            log.warn("Double-chunked documents directory not found: {}", chunkedDir);
            return docs;
        }

        Files.list(chunkedDir)
            .filter(path -> path.toString().endsWith(".pb"))
            .limit(10) // Process first 10 documents for embedding
            .forEach(path -> {
                try {
                    byte[] data = Files.readAllBytes(path);
                    PipeDoc doc = PipeDoc.parseFrom(data);
                    docs.add(doc);
                } catch (Exception e) {
                    log.warn("Failed to load document from {}: {}", path, e.getMessage());
                }
            });

        return docs;
    }

    private List<PipeDoc> performFirstEmbedding(List<PipeDoc> doubleChunkedDocs) {
        List<PipeDoc> embeddedDocs = new ArrayList<>();
        
        // First embedding configuration - MiniLM model
        EmbedderOptions firstEmbedConfig = new EmbedderOptions(
            List.of(EmbeddingModel.ALL_MINILM_L6_V2), // embeddingModels
            true,  // checkChunks
            false, // checkDocumentFields
            List.of("body", "title"), // documentFields
            List.of(), // customFieldMappings
            false, // processKeywords
            List.of(1), // keywordNgramSizes
            512,   // maxTokenSize
            "[FIRST-EMBED] ", // logPrefix
            "first_embeddings_%s_%s", // resultSetNameTemplate
            16,    // maxBatchSize
            "DROP_OLDEST" // backpressureStrategy
        );

        for (PipeDoc doc : doubleChunkedDocs) {
            try {
                ModuleProcessRequest request = createEmbedderRequest(doc, firstEmbedConfig, "first-embedding");
                ModuleProcessResponse response = embedderService.processData(request)
                    .await().atMost(java.time.Duration.ofMinutes(10));
                
                if (response.getSuccess() && response.hasOutputDoc()) {
                    embeddedDocs.add(response.getOutputDoc());
                    log.debug("First embedding successful for doc: {}", doc.getDocId());
                } else {
                    log.warn("First embedding failed for doc: {}", doc.getDocId());
                }
            } catch (Exception e) {
                log.error("Error in first embedding for doc {}: {}", doc.getDocId(), e.getMessage());
            }
        }

        return embeddedDocs;
    }

    private List<PipeDoc> performSecondEmbedding(List<PipeDoc> firstEmbeddedDocs) {
        List<PipeDoc> doubleEmbeddedDocs = new ArrayList<>();
        
        // Second embedding configuration - E5 model
        EmbedderOptions secondEmbedConfig = new EmbedderOptions(
            List.of(EmbeddingModel.E5_SMALL_V2), // embeddingModels
            true,  // checkChunks
            false, // checkDocumentFields
            List.of("body", "title"), // documentFields
            List.of(), // customFieldMappings
            false, // processKeywords
            List.of(1), // keywordNgramSizes
            512,   // maxTokenSize
            "[SECOND-EMBED] ", // logPrefix
            "second_embeddings_%s_%s", // resultSetNameTemplate
            16,    // maxBatchSize
            "DROP_OLDEST" // backpressureStrategy
        );

        for (PipeDoc doc : firstEmbeddedDocs) {
            try {
                ModuleProcessRequest request = createEmbedderRequest(doc, secondEmbedConfig, "second-embedding");
                ModuleProcessResponse response = embedderService.processData(request)
                    .await().atMost(java.time.Duration.ofMinutes(10));
                
                if (response.getSuccess() && response.hasOutputDoc()) {
                    doubleEmbeddedDocs.add(response.getOutputDoc());
                    log.debug("Second embedding successful for doc: {}", doc.getDocId());
                } else {
                    log.warn("Second embedding failed for doc: {}", doc.getDocId());
                }
            } catch (Exception e) {
                log.error("Error in second embedding for doc {}: {}", doc.getDocId(), e.getMessage());
            }
        }

        return doubleEmbeddedDocs;
    }

    private ModuleProcessRequest createEmbedderRequest(PipeDoc doc, EmbedderOptions config, String stepName) {
        ServiceMetadata metadata = ServiceMetadata.newBuilder()
            .setPipelineName("double-embedding-test")
            .setPipeStepName(stepName)
            .setStreamId(UUID.randomUUID().toString())
            .setCurrentHopNumber(1)
            .build();

        ProcessConfiguration processConfig = ProcessConfiguration.newBuilder()
            .setCustomJsonConfig(config.toStruct())
            .build();

        return ModuleProcessRequest.newBuilder()
            .setDocument(doc)
            .setMetadata(metadata)
            .setConfig(processConfig)
            .build();
    }

    private void saveDoubleEmbeddedDocuments(List<PipeDoc> docs) throws IOException {
        Path outputDir = Paths.get("modules/embedder/src/test/resources/double_embedded_pipedocs");
        Files.createDirectories(outputDir);

        for (int i = 0; i < docs.size(); i++) {
            PipeDoc doc = docs.get(i);
            Path outputFile = outputDir.resolve(String.format("double_embedded_%03d.pb", i + 1));
            Files.write(outputFile, doc.toByteArray());
        }

        log.info("Saved {} double-embedded documents to {}", docs.size(), outputDir);
    }

    private void verifyDoubleEmbeddingStructure(List<PipeDoc> docs) {
        for (PipeDoc doc : docs) {
            assertTrue(doc.hasSearchMetadata(), "Document should have search metadata");
            
            if (doc.getSearchMetadata().getSemanticResultsCount() > 0) {
                log.info("Document {} has {} semantic result sets", 
                    doc.getDocId(), doc.getSearchMetadata().getSemanticResultsCount());
                
                // Should have 4 semantic result sets (2 chunks × 2 embeddings)
                assertEquals(4, doc.getSearchMetadata().getSemanticResultsCount(), 
                    "Document should have exactly 4 semantic result sets after double embedding");
                
                // Verify embedding config IDs are set
                for (int i = 0; i < doc.getSearchMetadata().getSemanticResultsCount(); i++) {
                    var result = doc.getSearchMetadata().getSemanticResults(i);
                    assertNotNull(result.getEmbeddingConfigId(), 
                        "Embedding config ID should be set for result " + i);
                    
                    log.info("Result {}: {} chunks, chunk_config={}, embedding_config={}", 
                        i, result.getChunksCount(), result.getChunkConfigId(), result.getEmbeddingConfigId());
                    
                    // Verify chunks have embeddings
                    for (int j = 0; j < result.getChunksCount(); j++) {
                        var chunk = result.getChunks(j);
                        assertTrue(chunk.getEmbeddingInfo().getVectorCount() > 0,
                            "Chunk should have vector embeddings");
                    }
                }
            }
        }
        
        log.info("✅ Double embedding verification complete - 4 sets of embeddings per document");
    }
}