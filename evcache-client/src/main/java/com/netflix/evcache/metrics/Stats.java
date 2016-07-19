package com.netflix.evcache.metrics;

import com.netflix.evcache.EVCache.Call;

public interface Stats {
    /**
     * This notifies the counter that a call was successfully completed and took
     * duration milliseconds to complete it.
     * 
     * @param operation
     *            - The EVCache.Call that was completed
     * @param duration
     *            - The time in milliseconds it took to complete the call.
     */
    void operationCompleted(Call operation, long duration);

    /**
     * A call to the counter indicating that there was cache Hit
     */
    void cacheHit(Call call);

    /**
     * A call to the counter indicating that there was cache Miss
     */
    void cacheMiss(Call call);
}
