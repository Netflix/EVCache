package com.netflix.evcache.metrics;

public interface Operation {

    public static enum TYPE {
        MILLI, MICRO, NANO
    }

    String getName();

    void setName(String name);

    void start();

    long getStartTime();

    void stop();

    long getDuration();
}
