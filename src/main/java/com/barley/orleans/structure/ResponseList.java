package com.barley.orleans.structure;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Class to wrap all individual responses with a status value
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ResponseList {
    @JsonProperty
    private Integer status = 200;
    @JsonProperty
    private List<Response> responses;

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public List<Response> getResponses() {
        return responses;
    }

    public void setResponses(List<Response> responses) {
        this.responses = responses;
    }
}
