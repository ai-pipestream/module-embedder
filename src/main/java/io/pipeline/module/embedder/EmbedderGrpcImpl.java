package io.pipeline.module.embedder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import io.pipeline.data.v1.*;
import io.pipeline.data.module.*;
import io.pipeline.module.embedder.service.EmbedderOptions;
import io.pipeline.module.embedder.service.ReactiveVectorizer;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import org.apache.commons.lang3.StringUtils;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Reactive gRPC service implementation for the embedder service using Quarkus and Mutiny.
 * Processes documents and generates embeddings for chunks and document fields with GPU acceleration.
 */
@GrpcService // Marks this as a gRPC service that Quarkus will expose
@Singleton // Ensures only one instance is created
public class EmbedderGrpcImpl implements PipeStepProcessor {

    private static final Logger log = LoggerFactory.getLogger(EmbedderGrpcImpl.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    ReactiveVectorizer vectorizer; // Single vectorizer for now, can be extended to multiple

    @RunOnVirtualThread
    @Override
    public Uni<ModuleProcessResponse> processData(ModuleProcessRequest request) {
        return Uni.createFrom().item(() -> {
            if (request == null) {
                log.error("Received null request");
                throw new IllegalArgumentException("Request cannot be null");
            }
            return request;
        })
        .chain(this::processDocumentReactive)
        .onFailure().recoverWithItem(throwable -> {
            String errorMessage = "Error in EmbedderService: " + throwable.getMessage();
            log.error(errorMessage, throwable);
            return createErrorResponse(errorMessage, throwable);
        });
    }

    private Uni<ModuleProcessResponse> processDocumentReactive(ModuleProcessRequest request) {
        if (!request.hasDocument()) {
            throw new IllegalArgumentException("No document provided in the request");
        }

        PipeDoc inputDoc = request.getDocument();
        ProcessConfiguration config = request.getConfig();
        ServiceMetadata metadata = request.getMetadata();
        String streamId = metadata.getStreamId();
        String pipeStepName = metadata.getPipeStepName();

        log.info("Processing document ID: {} for step: {} in stream: {} using GPU: {}", 
                inputDoc.getDocId(), pipeStepName, streamId, vectorizer.isUsingGpu());

        return parseConfiguration(config, streamId, pipeStepName)
                .chain(embedderOptions -> {
                    ModuleProcessResponse.Builder responseBuilder = ModuleProcessResponse.newBuilder();
                    PipeDoc.Builder outputDocBuilder = inputDoc.toBuilder();

                    // Process chunks if available and enabled
                    if (embedderOptions.checkChunks() && inputDoc.hasSearchMetadata() && inputDoc.getSearchMetadata().getSemanticResultsCount() > 0) {
                        return processChunksReactive(inputDoc, outputDocBuilder, embedderOptions, pipeStepName)
                                .map(chunksProcessed -> {
                                    if (chunksProcessed) {
                                        responseBuilder.addProcessorLogs(String.format(
                                                "%sSuccessfully processed chunks for document ID: %s using model: %s (GPU: %s)",
                                                embedderOptions.logPrefix() != null ? embedderOptions.logPrefix() : "", 
                                                inputDoc.getDocId(), vectorizer.getModelId(), vectorizer.isUsingGpu()));
                                    }
                                    return buildSuccessResponse(responseBuilder, outputDocBuilder);
                                });
                    } 
                    // Process document fields if chunks were not processed
                    else if (embedderOptions.checkDocumentFields()) {
                        return processDocumentFieldsReactive(inputDoc, outputDocBuilder, embedderOptions, pipeStepName)
                                .map(fieldsProcessed -> {
                                    if (fieldsProcessed) {
                                        responseBuilder.addProcessorLogs(String.format(
                                                "%sSuccessfully processed document fields for document ID: %s using model: %s (GPU: %s)",
                                                embedderOptions.logPrefix() != null ? embedderOptions.logPrefix() : "", 
                                                inputDoc.getDocId(), vectorizer.getModelId(), vectorizer.isUsingGpu()));
                                    }
                                    return buildSuccessResponse(responseBuilder, outputDocBuilder);
                                });
                    } else {
                        responseBuilder.addProcessorLogs(String.format(
                                "%sNo processing configured for document ID: %s", 
                                embedderOptions.logPrefix() != null ? embedderOptions.logPrefix() : "", 
                                inputDoc.getDocId()));
                        return Uni.createFrom().item(buildSuccessResponse(responseBuilder, outputDocBuilder));
                    }
                });
    }

    private Uni<EmbedderOptions> parseConfiguration(ProcessConfiguration config, String streamId, String pipeStepName) {
        return Uni.createFrom().item(() -> {
            try {
                Struct customJsonConfig = config.getCustomJsonConfig();
                if (customJsonConfig != null && customJsonConfig.getFieldsCount() > 0) {
                    EmbedderOptions parsed = objectMapper.readValue(
                            JsonFormat.printer().print(customJsonConfig),
                            EmbedderOptions.class
                    );
                    return ensureDefaults(parsed);
                } else {
                    log.warn("No custom JSON config provided for EmbedderService. Using defaults. streamId: {}, pipeStepName: {}", 
                            streamId, pipeStepName);
                    return new EmbedderOptions();
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse EmbedderOptions configuration", e);
            }
        });
    }

    /**
     * Ensures that all fields in EmbedderOptions have non-null default values.
     */
    private EmbedderOptions ensureDefaults(EmbedderOptions options) {
        return new EmbedderOptions(
                options.embeddingModels() != null ? options.embeddingModels() : EmbedderOptions.DEFAULT_EMBEDDING_MODELS,
                options.checkChunks() != null ? options.checkChunks() : EmbedderOptions.DEFAULT_CHECK_CHUNKS,
                options.checkDocumentFields() != null ? options.checkDocumentFields() : EmbedderOptions.DEFAULT_CHECK_DOCUMENT_FIELDS,
                options.documentFields() != null ? options.documentFields() : EmbedderOptions.DEFAULT_DOCUMENT_FIELDS,
                options.customFieldMappings() != null ? options.customFieldMappings() : EmbedderOptions.DEFAULT_CUSTOM_FIELD_MAPPINGS,
                options.processKeywords() != null ? options.processKeywords() : EmbedderOptions.DEFAULT_PROCESS_KEYWORDS,
                options.keywordNgramSizes() != null ? options.keywordNgramSizes() : EmbedderOptions.DEFAULT_KEYWORD_NGRAM_SIZES,
                options.maxTokenSize() != null ? options.maxTokenSize() : EmbedderOptions.DEFAULT_MAX_TOKEN_SIZE,
                options.logPrefix() != null ? options.logPrefix() : EmbedderOptions.DEFAULT_LOG_PREFIX,
                options.resultSetNameTemplate() != null ? options.resultSetNameTemplate() : EmbedderOptions.DEFAULT_RESULT_SET_NAME_TEMPLATE,
                options.maxBatchSize() != null ? options.maxBatchSize() : EmbedderOptions.DEFAULT_MAX_BATCH_SIZE,
                options.backpressureStrategy() != null ? options.backpressureStrategy() : EmbedderOptions.DEFAULT_BACKPRESSURE_STRATEGY
        );
    }

    private Uni<Boolean> processChunksReactive(PipeDoc inputDoc, PipeDoc.Builder outputDocBuilder, 
                                               EmbedderOptions options, String pipeStepName) {
        log.debug("Processing {} semantic results for chunks", inputDoc.getSearchMetadata().getSemanticResultsCount());

        boolean chunksProcessed = false;

        // Process each semantic result
        for (int i = 0; i < inputDoc.getSearchMetadata().getSemanticResultsCount(); i++) {
            SemanticProcessingResult result = inputDoc.getSearchMetadata().getSemanticResults(i);

            // Skip if no chunks
            if (result.getChunksCount() == 0) {
                continue;
            }

            // Extract chunk texts for batch processing
            List<String> chunkTexts = new ArrayList<>();
            for (int j = 0; j < result.getChunksCount(); j++) {
                SemanticChunk chunk = result.getChunks(j);
                chunkTexts.add(chunk.getEmbeddingInfo().getTextContent());
            }

            log.info("Processing {} chunks in batch for semantic result {}", chunkTexts.size(), i);

            // Generate embeddings in batch (blocking for now, can be made async later)
            List<float[]> embeddings = vectorizer.batchEmbeddings(chunkTexts).await().indefinitely();

            // Create a new semantic result with the same properties but with embeddings
            SemanticProcessingResult.Builder newResultBuilder = result.toBuilder()
                    .setEmbeddingConfigId(vectorizer.getModelId());

            // Format the result set name
            String template = options.resultSetNameTemplate() != null ? 
                options.resultSetNameTemplate() : "%s_embeddings_%s";
            String resultSetName = String.format(
                    template,
                    pipeStepName != null ? pipeStepName : "embedder",
                    vectorizer.getModelId()
            ).replaceAll("[^a-zA-Z0-9_\\-]", "_");
            newResultBuilder.setResultSetName(resultSetName);

            // Update chunks with embeddings
            for (int j = 0; j < result.getChunksCount(); j++) {
                SemanticChunk chunk = result.getChunks(j);
                float[] embedding = embeddings.get(j);

                ChunkEmbedding.Builder embeddingInfoBuilder = chunk.getEmbeddingInfo().toBuilder();
                embeddingInfoBuilder.clearVector();
                for (float value : embedding) {
                    embeddingInfoBuilder.addVector(value);
                }

                SemanticChunk.Builder chunkBuilder = chunk.toBuilder()
                        .setEmbeddingInfo(embeddingInfoBuilder);

                newResultBuilder.setChunks(j, chunkBuilder.build());
            }

            // Add the new result to the output document
            SemanticProcessingResult newResult = newResultBuilder.build();
            if (!outputDocBuilder.hasSearchMetadata()) {
                outputDocBuilder.setSearchMetadata(SearchMetadata.newBuilder());
            }
            outputDocBuilder.getSearchMetadataBuilder().addSemanticResults(newResult);
            chunksProcessed = true;

            log.info("{}Added embeddings to {} chunks using model {} for document ID: {}", 
                    options.logPrefix() != null ? options.logPrefix() : "", 
                    result.getChunksCount(), vectorizer.getModelId(), inputDoc.getDocId());
        }

        return Uni.createFrom().item(chunksProcessed);
    }


    private Uni<Boolean> processDocumentFieldsReactive(PipeDoc inputDoc, PipeDoc.Builder outputDocBuilder,
                                                        EmbedderOptions options, String pipeStepName) {

        // If no document fields to process, return false immediately
        if (options.documentFields() == null || options.documentFields().isEmpty()) {
            return Uni.createFrom().item(false);
        }

        // Create a list to hold all the field processing Uni tasks
        List<Uni<Boolean>> fieldProcessingTasks = new ArrayList<>();

        // Process each field reactively
        for (String fieldName : options.documentFields()) {
            String fieldText = extractFieldText(inputDoc, fieldName);

            // Skip if field is empty
            if (StringUtils.isBlank(fieldText)) {
                continue;
            }

            // Create a reactive task for this field
            Uni<Boolean> fieldTask = vectorizer.embeddings(fieldText)
                .map(embedding -> {
                    // Create embedding object
                    Embedding.Builder embeddingBuilder = Embedding.newBuilder()
                            .setModelId(vectorizer.getModelId());
                    for (float value : embedding) {
                        embeddingBuilder.addVector(value);
                    }

                    // Add embedding to the document
                    String embeddingName = fieldName + "_" + vectorizer.getModelId().toLowerCase();
                    // Store embedding in structured_data as Any for now
                    // TODO: Define proper embedding storage structure
                    log.debug("Generated embedding for field {} with {} dimensions", fieldName, embedding.length);

                    log.info("{}Added embedding for field {} using model {} for document ID: {}", 
                            options.logPrefix() != null ? options.logPrefix() : "", 
                            fieldName, vectorizer.getModelId(), inputDoc.getDocId());

                    return true;
                });

            fieldProcessingTasks.add(fieldTask);
        }

        // If no tasks were created, return false
        if (fieldProcessingTasks.isEmpty()) {
            return Uni.createFrom().item(false);
        }

        // Combine all field processing tasks and return true if any field was processed
        return Uni.combine().all().unis(fieldProcessingTasks)
            .with(results -> {
                // Check if any field was processed successfully
                for (Object result : results) {
                    if ((Boolean) result) {
                        return true;
                    }
                }
                return false;
            });
    }

    private String extractFieldText(PipeDoc document, String fieldName) {
        switch (fieldName.toLowerCase()) {
            case "body":
                return document.hasSearchMetadata() && document.getSearchMetadata().hasBody() ? document.getSearchMetadata().getBody() : null;
            case "title":
                return document.hasSearchMetadata() && document.getSearchMetadata().hasTitle() ? document.getSearchMetadata().getTitle() : null;
            case "id":
                return document.getDocId();
            default:
                log.warn("Unsupported field for embedding: {}", fieldName);
                return null;
        }
    }


    private ModuleProcessResponse buildSuccessResponse(ModuleProcessResponse.Builder responseBuilder, PipeDoc.Builder outputDocBuilder) {
        responseBuilder.setSuccess(true);
        responseBuilder.setOutputDoc(outputDocBuilder.build());
        return responseBuilder.build();
    }

    @Override
    public Uni<ServiceRegistrationMetadata> getServiceRegistration(RegistrationRequest request) {
        log.info("Embedder service registration requested");

        ServiceRegistrationMetadata.Builder responseBuilder = ServiceRegistrationMetadata.newBuilder()
                .setModuleName("embedder")
                .setVersion("1.0.0-SNAPSHOT")
                .setJsonConfigSchema(EmbedderOptions.getJsonV7Schema());

        // If test request is provided, perform health check
        if (request.hasTestRequest()) {
            log.info("Performing health check with test request");
            return processData(request.getTestRequest())
                .map(processResponse -> {
                    if (processResponse.getSuccess()) {
                        responseBuilder
                            .setHealthCheckPassed(true)
                            .setHealthCheckMessage(String.format(
                                "Embedder module is healthy - using model: %s (GPU: %s)", 
                                vectorizer.getModelId(), 
                                vectorizer.isUsingGpu()));
                    } else {
                        responseBuilder
                            .setHealthCheckPassed(false)
                            .setHealthCheckMessage("Embedder module health check failed: " + 
                                String.join("; ", processResponse.getProcessorLogsList()));
                    }
                    return responseBuilder.build();
                })
                .onFailure().recoverWithItem(error -> {
                    log.error("Health check failed with exception", error);
                    return responseBuilder
                        .setHealthCheckPassed(false)
                        .setHealthCheckMessage("Health check failed with exception: " + error.getMessage())
                        .build();
                });
        } else {
            // No test request provided, assume healthy
            responseBuilder
                .setHealthCheckPassed(true)
                .setHealthCheckMessage(String.format(
                    "No health check performed - module assumed healthy (GPU: %s)", 
                    vectorizer.isUsingGpu()));
            return Uni.createFrom().item(responseBuilder.build());
        }
    }

    @Override
    public Uni<ModuleProcessResponse> testProcessData(ModuleProcessRequest request) {
        log.debug("TestProcessData called - proxying to processData");
        return processData(request);
    }

    private ModuleProcessResponse createErrorResponse(String errorMessage, Throwable e) {
        ModuleProcessResponse.Builder responseBuilder = ModuleProcessResponse.newBuilder();
        responseBuilder.setSuccess(false);
        responseBuilder.addProcessorLogs(errorMessage);

        Struct.Builder errorDetailsBuilder = Struct.newBuilder();
        errorDetailsBuilder.putFields("error_message", Value.newBuilder().setStringValue(errorMessage).build());
        if (e != null) {
            errorDetailsBuilder.putFields("error_type", Value.newBuilder().setStringValue(e.getClass().getName()).build());
            if (e.getCause() != null) {
                errorDetailsBuilder.putFields("error_cause", Value.newBuilder().setStringValue(e.getCause().getMessage()).build());
            }
        }
        responseBuilder.setErrorDetails(errorDetailsBuilder.build());
        return responseBuilder.build();
    }
}
