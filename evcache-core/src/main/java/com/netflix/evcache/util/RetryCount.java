package com.netflix.evcache.util;

public class RetryCount {
    private int retryCount;
    public RetryCount() {
        retryCount = 1;
    }
    public void incr() {
        retryCount++;
    }
    public int get(){
        return retryCount;
    }
}
