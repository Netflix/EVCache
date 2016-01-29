package com.netflix.evcache;

public class EVCacheException extends Exception {

    private static final long serialVersionUID = -3885811159646046383L;

    public EVCacheException(String message) {
        super(message);
    }

    public EVCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}