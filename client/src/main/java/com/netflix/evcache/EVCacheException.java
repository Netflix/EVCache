package com.netflix.evcache;


/**
 * Base exception class for any error conditions that occur while using an EVCache client 
 * to make service calls to EVCache Server.
 */
public class EVCacheException extends Exception {
    
    private static final long serialVersionUID = -3885811159646046383L;

    /**
     * Constructs a new EVCacheException with the specified message.
     * 
     * @param message Describes the error encountered.
     */
    public EVCacheException(String message) {
        super(message);
    }

    /**
     * Constructs a new EVCacheException with the specified message and exception indicating the root cause.
     * 
     * @param message Describes the error encountered.
     * @param cause The root exception that caused this exception to be thrown.
     */
    public EVCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}