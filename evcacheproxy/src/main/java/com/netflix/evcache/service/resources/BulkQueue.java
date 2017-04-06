package com.netflix.evcache.service.resources;

/**
 * Created by senugula on 4/6/17.
 */
public class BulkQueue {

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    private String appId;
    private String input;

    public String getTtl() {
        return ttl;
    }

    public void setTtl(String ttl) {
        this.ttl = ttl;
    }

    private String ttl;

    public BulkQueue() {
    }
}
