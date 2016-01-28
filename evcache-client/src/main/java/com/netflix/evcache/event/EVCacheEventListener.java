package com.netflix.evcache.event;

import java.util.EventListener;

public interface EVCacheEventListener extends EventListener {

    void onStart(EVCacheEvent e);

    void onComplete(EVCacheEvent e);

    void onError(EVCacheEvent e, Throwable t);

    boolean onThrottle(EVCacheEvent e);
}