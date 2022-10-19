package com.netflix.evcache.dto;

public class EVCacheResponseStatus {
    private String status;

    public EVCacheResponseStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
