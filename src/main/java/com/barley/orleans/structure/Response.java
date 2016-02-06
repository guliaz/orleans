package com.barley.orleans.structure;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to wrap individual response from produce method
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Response {
    @JsonProperty
    private Integer partition;
    @JsonProperty
    private Long offset;
    @JsonProperty
    private List<String> errors = new ArrayList<>();

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setErrors(List<String> errors) {
        this.errors = errors;
    }

    public void addError(String error) {
        this.errors.add(error);
    }

    @Override
    public String toString() {
        return "Response{" +
                "partition=" + partition +
                ", offset=" + offset +
                ", errors=" + errors +
                '}';
    }
}
