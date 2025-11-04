package io.pipeline.module.embedder.service;

import io.smallrye.mutiny.Uni;

import java.util.List;

/**
 * Reactive interface for vectorizing text into embeddings using Mutiny.
 * Supports both single and batch operations with reactive programming patterns.
 */
public interface Vectorizer {

    /**
     * Generates a vector embedding for the given text.
     *
     * @param text The text to vectorize
     * @return A Uni containing a float array representing the embedding
     */
    Uni<float[]> embeddings(String text);

    /**
     * Generates vector embeddings for a batch of text inputs.
     * This method optimizes performance by processing multiple texts in parallel batches.
     *
     * @param texts The list of input texts to be vectorized
     * @return A Uni containing a list of float arrays representing the embeddings
     */
    Uni<List<float[]>> batchEmbeddings(List<String> texts);

    /**
     * Gets the model ID used by this vectorizer.
     * 
     * @return the model ID
     */
    String getModelId();
    
    /**
     * Gets the embedding model configuration.
     * 
     * @return the embedding model
     */
    EmbeddingModel getModel();
    
    /**
     * Checks if this vectorizer is using GPU acceleration.
     * 
     * @return true if using GPU, false if using CPU
     */
    boolean isUsingGpu();
    
    /**
     * Gets the maximum batch size for optimal performance.
     * 
     * @return the maximum batch size
     */
    int getMaxBatchSize();
}