package com.netflix.evcache;

import java.io.IOException;

public class EVCacheConnectException extends IOException {

    private static final long serialVersionUID = 8065483548278456469L;

    public EVCacheConnectException(String message) {
        super(message);
    }

    public EVCacheConnectException(String message, Throwable cause) {
        super(message, cause);
    }
}