package io.pipeline.module.embedder.service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration of supported embedding models.
 * Each model includes its DJL URI and a description.
 */
public enum EmbeddingModel {
    
    /**
     * Lightweight and fast, great for general-purpose sentence embeddings.
     */
    ALL_MINILM_L6_V2("djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2", 
            "Lightweight and fast, great for general-purpose sentence embeddings."),
    
    /**
     * Higher accuracy than MiniLM, though a bit larger and slower.
     */
    ALL_MPNET_BASE_V2("djl://ai.djl.huggingface.pytorch/sentence-transformers/all-mpnet-base-v2", 
            "Higher accuracy than MiniLM, though a bit larger and slower."),
    
    /**
     * Based on DistilRoBERTa; good balance between performance and speed.
     */
    ALL_DISTILROBERTA_V1("djl://ai.djl.huggingface.pytorch/sentence-transformers/all-distilroberta-v1", 
            "Based on DistilRoBERTa; good balance between performance and speed."),
    
    /**
     * Even smaller and faster than L6 or L12 versions; ideal for low-latency scenarios.
     */
    PARAPHRASE_MINILM_L3_V2("djl://ai.djl.huggingface.pytorch/sentence-transformers/paraphrase-MiniLM-L3-v2", 
            "Even smaller and faster than L6 or L12 versions; ideal for low-latency scenarios."),
    
    /**
     * Multilingual support (50+ languages) + small model size.
     */
    PARAPHRASE_MULTILINGUAL_MINILM_L12_V2("djl://ai.djl.huggingface.pytorch/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", 
            "Multilingual support (50+ languages) + small model size."),
    
    /**
     * Smaller sibling of e5-base-v2, good for retrieval tasks, especially with query-document use cases.
     */
    E5_SMALL_V2("djl://ai.djl.huggingface.pytorch/sentence-transformers/e5-small-v2", 
            "Smaller sibling of e5-base-v2, good for retrieval tasks, especially with query-document use cases."),
    
    /**
     * Larger and more accurate than e5-base-v2, better embeddings at the cost of speed/memory.
     */
    E5_LARGE_V2("djl://ai.djl.huggingface.pytorch/sentence-transformers/e5-large-v2", 
            "Larger and more accurate than e5-base-v2, better embeddings at the cost of speed/memory."),
    
    /**
     * Fine-tuned for semantic search and QA.
     */
    MULTI_QA_MINILM_L6_COS_V1("djl://ai.djl.huggingface.pytorch/sentence-transformers/multi-qa-MiniLM-L6-cos-v1", 
            "Fine-tuned for semantic search and QA.");
    
    private final String uri;
    private final String description;
    
    EmbeddingModel(String uri, String description) {
        this.uri = uri;
        this.description = description;
    }
    
    /**
     * Gets the DJL URI for this model.
     * 
     * @return the DJL URI
     */
    public String getUri() {
        return uri;
    }
    
    /**
     * Gets the description of this model.
     * 
     * @return the description
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * Returns the enum value as a string for JSON serialization.
     * 
     * @return the name of the enum value
     */
    @JsonValue
    public String toValue() {
        return this.name();
    }
    
    /**
     * Creates an enum value from a string for JSON deserialization.
     * 
     * @param value the string value
     * @return the corresponding enum value
     * @throws IllegalArgumentException if the string value does not match any enum value
     */
    @JsonCreator
    public static EmbeddingModel fromValue(String value) {
        try {
            return EmbeddingModel.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown embedding model: " + value);
        }
    }
}