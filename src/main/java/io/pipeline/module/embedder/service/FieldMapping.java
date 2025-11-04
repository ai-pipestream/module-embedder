package io.pipeline.module.embedder.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a mapping from a source field to a target field for embedding.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FieldMapping {
    private final String sourceField;
    private final String targetField;

    public FieldMapping(
            @JsonProperty("source_field") String sourceField,
            @JsonProperty("target_field") String targetField) {
        this.sourceField = sourceField;
        this.targetField = targetField;
    }

    public String sourceField() {
        return sourceField;
    }

    public String targetField() {
        return targetField;
    }
}