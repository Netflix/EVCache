package com.netflix.evcache;

public class EVCacheMissException extends EVCacheException {

    private static final long serialVersionUID = 222337840463312890L;

    public EVCacheMissException(String message) {
        super(message);
    }

    public EVCacheMissException(String message, Throwable cause) {
        super(message, cause);
    }

}
