package com.netflix.evcache;

public class EVCacheReadQueueException extends EVCacheException {

    private static final long serialVersionUID = -7660503904923117538L;

    public EVCacheReadQueueException(String message) {
        super(message);
    }

    public EVCacheReadQueueException(String message, Throwable cause) {
        super(message, cause);
    }
}