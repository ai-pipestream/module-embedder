package io.pipeline.module.embedder;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.pipeline.data.v1.*;
import io.pipeline.data.module.*;
import io.pipeline.data.util.proto.ProtobufTestDataHelper;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.Test;
import org.jboss.logging.Logger;

import java.util.Collection;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


public abstract class EmbedderServiceTestBase {

    private static final Logger log = Logger.getLogger(EmbedderServiceTestBase.class);

    protected abstract PipeStepProcessor getEmbedderService();

    @Test
    void testProcessData() {
        // Create a test document
        PipeDoc testDoc = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("This is a test document body")
                        .setTitle("Test Document")
                        .build())
                .build();

        // Create service metadata
        ServiceMetadata metadata = ServiceMetadata.newBuilder()
                .setPipelineName("test-pipeline")
                .setPipeStepName("embedder-step")
                .setStreamId(UUID.randomUUID().toString())
                .setCurrentHopNumber(1)
                .putContextParams("tenant", "test-tenant")
                .build();

        // Create configuration
        ProcessConfiguration config = ProcessConfiguration.newBuilder()
                .setCustomJsonConfig(Struct.newBuilder()
                        .putFields("check_chunks", Value.newBuilder().setBoolValue(true).build())
                        .putFields("check_document_fields", Value.newBuilder().setBoolValue(true).build())
                        .build())
                .build();

        // Create request
        ModuleProcessRequest request = ModuleProcessRequest.newBuilder()
                .setDocument(testDoc)
                .setMetadata(metadata)
                .setConfig(config)
                .build();

        // Execute and verify
        var response = getEmbedderService().processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem();

        assertThat("Embedder should successfully process document", response.getSuccess(), is(true));
        assertThat("Response should contain output document", response.hasOutputDoc(), is(true));
        assertThat("Output document ID should match input", response.getOutputDoc().getDocId(), is(equalTo(testDoc.getDocId())));
        assertThat("Should generate processor logs", response.getProcessorLogsList(), is(not(empty())));
        assertThat("Should log document field processing", response.getProcessorLogsList(), hasItem(containsString("processed document fields")));
    }

    @Test
    void testProcessDataWithoutDocument() {
        // Test with no document - should still succeed but with error logs
        ModuleProcessRequest request = ModuleProcessRequest.newBuilder()
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("embedder-step")
                        .build())
                .setConfig(ProcessConfiguration.newBuilder().build())
                // No document set
                .build();

        var response = getEmbedderService().processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem();

        assertThat("Embedder should fail gracefully without document", response.getSuccess(), is(false));
        assertThat("Should provide error logs", response.getProcessorLogsList(), is(not(empty())));
        assertThat("Should log embedder service error", response.getProcessorLogsList(), hasItem(containsString("Error in EmbedderService")));
    }

    @Test
    void testGetServiceRegistrationWithoutHealthCheck() {
        RegistrationRequest request = RegistrationRequest.newBuilder().build();
        var registration = getEmbedderService().getServiceRegistration(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem();

        assertThat("Service should identify as embedder module", registration.getModuleName(), is(equalTo("embedder")));
        assertThat("Registration should include configuration schema", registration.hasJsonConfigSchema(), is(true));
        assertThat("Schema should reference EmbedderOptions", registration.getJsonConfigSchema(), containsString("EmbedderOptions"));
        assertThat("Health check should pass without test request", registration.getHealthCheckPassed(), is(true));
        assertThat("Should indicate no health check performed", registration.getHealthCheckMessage(), containsString("No health check performed"));
    }

    @Test
    void testGetServiceRegistrationWithHealthCheck() {
        // Create a test document for health check
        PipeDoc testDoc = PipeDoc.newBuilder()
                .setDocId("health-check-doc")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("Health check test for embedder")
                        .setTitle("Health Check")
                        .build())
                .build();

        ModuleProcessRequest processRequest = ModuleProcessRequest.newBuilder()
                .setDocument(testDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("health-check")
                        .setPipeStepName("embedder-health")
                        .setStreamId("health-check-stream")
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setCustomJsonConfig(Struct.newBuilder()
                                .putFields("check_document_fields", Value.newBuilder().setBoolValue(true).build())
                                .build())
                        .build())
                .build();

        // Call with test request for health check
        RegistrationRequest request = RegistrationRequest.newBuilder()
                .setTestRequest(processRequest)
                .build();
        
        var registration = getEmbedderService().getServiceRegistration(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem();
        
        assertThat("Service should identify as embedder module", registration.getModuleName(), is(equalTo("embedder")));
        assertThat("Registration should include configuration schema", registration.hasJsonConfigSchema(), is(true));
        assertThat("Health check should pass with test document", registration.getHealthCheckPassed(), is(true));
        assertThat("Should confirm embedder module health", registration.getHealthCheckMessage(), containsString("Embedder module is healthy"));
    }

    @Test
    void testProcessChunkerOutputData() {
        // Get chunker output data from test-utilities
        ProtobufTestDataHelper testHelper = new ProtobufTestDataHelper();
        Collection<PipeStream> chunkerOutputStreams = testHelper.getChunkerPipeStreams();

        // If no chunker output data is available, log a warning and skip the test
        if (chunkerOutputStreams.isEmpty()) {
            log.warn("No chunker output data available for testing. Skipping test.");
            return;
        }

        // Use the first chunker output stream for testing
        PipeStream chunkerOutputStream = chunkerOutputStreams.iterator().next();
        PipeDoc chunkerOutputDoc = chunkerOutputStream.getDocument();

        log.infof("Testing embedder with chunker output document ID: %s", chunkerOutputDoc.getDocId());
        log.infof("Chunker output document has %d semantic results", chunkerOutputDoc.hasSearchMetadata() ? chunkerOutputDoc.getSearchMetadata().getSemanticResultsCount() : 0);

        // Create service metadata
        ServiceMetadata metadata = ServiceMetadata.newBuilder()
                .setPipelineName("test-pipeline")
                .setPipeStepName("embedder-step")
                .setStreamId(chunkerOutputStream.getStreamId())
                .setCurrentHopNumber(1)
                .putContextParams("tenant", "test-tenant")
                .build();

        // Create configuration with chunk processing enabled
        ProcessConfiguration config = ProcessConfiguration.newBuilder()
                .setCustomJsonConfig(Struct.newBuilder()
                        .putFields("check_chunks", Value.newBuilder().setBoolValue(true).build())
                        .putFields("check_document_fields", Value.newBuilder().setBoolValue(true).build())
                        .build())
                .build();

        // Create request with chunker output document
        ModuleProcessRequest request = ModuleProcessRequest.newBuilder()
                .setDocument(chunkerOutputDoc)
                .setMetadata(metadata)
                .setConfig(config)
                .build();

        // Execute and verify
        var response = getEmbedderService().processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem();

        // Verify the response
        assertThat("Embedder should successfully process chunker output", response.getSuccess(), is(true));
        assertThat("Response should contain embedded output document", response.hasOutputDoc(), is(true));
        assertThat("Output document ID should be preserved", response.getOutputDoc().getDocId(), is(equalTo(chunkerOutputDoc.getDocId())));
        assertThat("Should log embedding processing steps", response.getProcessorLogsList(), is(not(empty())));

        // Verify that chunks were processed
        PipeDoc outputDoc = response.getOutputDoc();
        assertThat("Embedded document should contain semantic results", outputDoc.hasSearchMetadata() ? outputDoc.getSearchMetadata().getSemanticResultsCount() : 0, is(greaterThan(0)));

        // Log the results
        log.info("Embedder successfully processed chunker output document");
        log.infof("Output document has %d semantic results", outputDoc.hasSearchMetadata() ? outputDoc.getSearchMetadata().getSemanticResultsCount() : 0);

        // Verify that embeddings were added to the chunks
        for (int i = 0; i < outputDoc.getSearchMetadata().getSemanticResultsCount(); i++) {
            SemanticProcessingResult result = outputDoc.getSearchMetadata().getSemanticResults(i);
            log.infof("Semantic result %d has %d chunks", i, result.getChunksCount());

            for (int j = 0; j < result.getChunksCount(); j++) {
                SemanticChunk chunk = result.getChunks(j);
                ChunkEmbedding embeddingInfo = chunk.getEmbeddingInfo();

                // Verify that the chunk has a vector embedding
                assertThat(String.format("Chunk %d should have vector embeddings", j), embeddingInfo.getVectorCount(), is(greaterThan(0)));
                log.infof("Chunk %d has %d vector dimensions", j, embeddingInfo.getVectorCount());
            }
        }
    }
}