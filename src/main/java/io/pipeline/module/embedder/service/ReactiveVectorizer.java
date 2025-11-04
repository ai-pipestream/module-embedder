package io.pipeline.module.embedder.service;

import ai.djl.Device;
import ai.djl.MalformedModelException;
import ai.djl.engine.Engine;
import ai.djl.huggingface.translator.TextEmbeddingTranslatorFactory;
import ai.djl.inference.Predictor;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.TranslateException;
import io.quarkus.runtime.Startup;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reactive implementation of the Vectorizer interface using DJL (Deep Java Library).
 * This class manages GPU/CPU resources efficiently with reactive patterns and virtual threads.
 */
@Singleton
@Startup
public class ReactiveVectorizer implements Vectorizer {

    private static final Logger log = LoggerFactory.getLogger(ReactiveVectorizer.class);
    
    private final EmbeddingModel model;
    private final String modelId;
    private final ZooModel<String, float[]> djlModel;
    private final BlockingQueue<Predictor<String, float[]>> predictorPool;
    private final int poolSize;
    private final int maxBatchSize;
    private final boolean usingGpu;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    
    // Executor for CPU-bound ML operations
    private final ExecutorService mlExecutor;
    
    /**
     * Creates a ReactiveVectorizer with the default model (ALL_MINILM_L6_V2).
     */
    public ReactiveVectorizer() throws ModelNotFoundException, MalformedModelException, IOException {
        this(EmbeddingModel.ALL_MINILM_L6_V2, 4, 32);
    }

    /**
     * Creates a ReactiveVectorizer with the specified configuration.
     *
     * @param model the embedding model to use
     * @param poolSize the size of the predictor pool
     * @param maxBatchSize the maximum batch size for processing
     */
    public ReactiveVectorizer(EmbeddingModel model, int poolSize, int maxBatchSize) 
            throws ModelNotFoundException, MalformedModelException, IOException {
        this.model = model;
        this.modelId = model.name();
        this.poolSize = poolSize;
        this.maxBatchSize = maxBatchSize;
        
        log.info("Initializing ReactiveVectorizer for model: {} with pool size: {}, max batch size: {}", 
                modelId, poolSize, maxBatchSize);
        
        // Load the DJL model with device selection
        Device device = selectOptimalDevice();
        this.usingGpu = device.isGpu();
        this.djlModel = loadModel(model.getUri(), device);
        this.predictorPool = createPredictorPool();
        
        // Create a dedicated executor for ML operations
        this.mlExecutor = createMLExecutor();
        
        log.info("ReactiveVectorizer initialized successfully. Using device: {}, GPU: {}", 
                device, usingGpu);
    }

    private Device selectOptimalDevice() {
        int gpuCount = Engine.getInstance().getGpuCount();
        if (gpuCount > 0) {
            try {
                Device gpu = Device.gpu();
                log.info("CUDA GPU detected and available. GPU count: {}. Using GPU: {}", gpuCount, gpu);
                return gpu;
            } catch (Exception e) {
                log.warn("GPU detected but failed to initialize CUDA device: {}", e.getMessage());
            }
        }
        
        Device cpu = Device.cpu();
        log.info("Using CPU device for ML inference: {}", cpu);
        return cpu;
    }

    private ZooModel<String, float[]> loadModel(String modelUrl, Device device) 
            throws ModelNotFoundException, MalformedModelException, IOException {
        log.info("Loading DJL model from: {} on device: {}", modelUrl, device);
        
        Criteria<String, float[]> criteria = Criteria.builder()
                .setTypes(String.class, float[].class)
                .optModelUrls(modelUrl)
                .optEngine("PyTorch")
                .optDevice(device)
                .optTranslatorFactory(new TextEmbeddingTranslatorFactory())
                .build();

        ZooModel<String, float[]> loadedModel = criteria.loadModel();
        log.info("Model loaded successfully: {}", modelId);
        return loadedModel;
    }

    private BlockingQueue<Predictor<String, float[]>> createPredictorPool() {
        BlockingQueue<Predictor<String, float[]>> pool = new ArrayBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            Predictor<String, float[]> predictor = djlModel.newPredictor();
            pool.offer(predictor);
        }
        log.info("Created predictor pool with {} predictors", poolSize);
        return pool;
    }

    private ExecutorService createMLExecutor() {
        // Use virtual threads for better resource utilization
        return Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
                .name("ml-vectorizer-", 0)
                .factory());
    }

    @Override
    public Uni<float[]> embeddings(String text) {
        if (text == null || text.trim().isEmpty()) {
            log.warn("Attempted to vectorize null or empty text");
            return Uni.createFrom().item(new float[0]);
        }

        if (isShutdown.get()) {
            return Uni.createFrom().failure(new IllegalStateException("Vectorizer has been shut down"));
        }

        return Uni.createFrom().item(text)
                .emitOn(Infrastructure.getDefaultExecutor())
                .chain(inputText -> {
                    return Uni.createFrom().completionStage(() -> 
                        CompletableFuture.supplyAsync(() -> {
                            Predictor<String, float[]> predictor = null;
                            try {
                                predictor = predictorPool.take();
                                float[] result = predictor.predict(inputText);
                                log.debug("Generated embedding for text (length: {}) -> embedding size: {}", 
                                        inputText.length(), result.length);
                                return result;
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException("Thread interrupted while waiting for predictor", e);
                            } catch (TranslateException e) {
                                throw new RuntimeException("Error generating embedding", e);
                            } finally {
                                if (predictor != null) {
                                    predictorPool.offer(predictor);
                                }
                            }
                        }, mlExecutor)
                    );
                });
    }

    @Override
    public Uni<List<float[]>> batchEmbeddings(List<String> texts) {
        if (texts == null || texts.isEmpty()) {
            log.warn("Attempted to vectorize null or empty batch");
            return Uni.createFrom().item(Collections.emptyList());
        }

        if (isShutdown.get()) {
            return Uni.createFrom().failure(new IllegalStateException("Vectorizer has been shut down"));
        }

        log.debug("Processing batch of {} texts with max batch size: {}", texts.size(), maxBatchSize);

        return Uni.createFrom().item(texts)
                .emitOn(Infrastructure.getDefaultExecutor())
                .chain(inputTexts -> {
                    // Split into optimal batch sizes for GPU processing
                    List<List<String>> batches = createOptimalBatches(inputTexts);
                    
                    // Process batches in parallel using reactive streams
                    List<Uni<List<float[]>>> batchUnis = batches.stream()
                            .map(this::processBatch)
                            .toList();
                    
                    // Combine all batch results
                    return Uni.combine().all().unis(batchUnis)
                            .with(results -> {
                                List<float[]> allEmbeddings = new ArrayList<>();
                                for (Object result : results) {
                                    @SuppressWarnings("unchecked")
                                    List<float[]> batchResult = (List<float[]>) result;
                                    allEmbeddings.addAll(batchResult);
                                }
                                log.debug("Completed batch processing: {} texts -> {} embeddings", 
                                        inputTexts.size(), allEmbeddings.size());
                                return allEmbeddings;
                            });
                });
    }

    private List<List<String>> createOptimalBatches(List<String> texts) {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < texts.size(); i += maxBatchSize) {
            int end = Math.min(i + maxBatchSize, texts.size());
            batches.add(texts.subList(i, end));
        }
        log.debug("Split {} texts into {} batches of max size {}", texts.size(), batches.size(), maxBatchSize);
        return batches;
    }

    private Uni<List<float[]>> processBatch(List<String> batch) {
        return Uni.createFrom().completionStage(() ->
                CompletableFuture.supplyAsync(() -> {
                    Predictor<String, float[]> predictor = null;
                    try {
                        predictor = predictorPool.take();
                        List<float[]> results = predictor.batchPredict(batch);
                        log.debug("Processed batch of size {} -> {} embeddings", batch.size(), results.size());
                        return results;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Thread interrupted during batch processing", e);
                    } catch (TranslateException e) {
                        throw new RuntimeException("Error during batch prediction", e);
                    } finally {
                        if (predictor != null) {
                            predictorPool.offer(predictor);
                        }
                    }
                }, mlExecutor)
        );
    }

    @Override
    public String getModelId() {
        return modelId;
    }

    @Override
    public EmbeddingModel getModel() {
        return model;
    }

    @Override
    public boolean isUsingGpu() {
        return usingGpu;
    }

    @Override
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    @PreDestroy
    public void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            log.info("Shutting down ReactiveVectorizer for model: {}", modelId);
            
            // Close all predictors in the pool
            predictorPool.forEach(Predictor::close);
            
            // Close the model
            if (djlModel != null) {
                djlModel.close();
            }
            
            // Shutdown the executor
            if (mlExecutor != null) {
                mlExecutor.shutdown();
                try {
                    if (!mlExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                        mlExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    mlExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            
            log.info("ReactiveVectorizer shutdown completed for model: {}", modelId);
        }
    }
}