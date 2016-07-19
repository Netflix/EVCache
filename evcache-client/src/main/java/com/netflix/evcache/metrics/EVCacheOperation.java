package com.netflix.evcache.metrics;

import com.netflix.evcache.EVCache.Call;

public class EVCacheOperation implements Operation {
    private final Call op;
    private final Stats stats;
    private long startTime;
    private final String name;
    private long duration;
    private final Operation.TYPE type;

    public EVCacheOperation(String name) {
        this.op = null;
        this.name = name;
        this.stats = null;
        this.type = Operation.TYPE.MILLI;
    }

    public EVCacheOperation(String name, Call op, Stats stats, Operation.TYPE type) {
        this.op = op;
        this.stats = stats;
        startTime = System.nanoTime();
        this.type = type;
        this.name = (op == null) ? name : name + ":" + op.name();

    }

    public void setName(String name) {
    }

    public void start() {
        startTime = System.nanoTime();
    }

    public long getStartTime() {
        return startTime;
    }

    public void stop() {
        duration = System.nanoTime() - startTime;
        if (op != null && stats != null) stats.operationCompleted(op, getDuration());
    }

    public long getDuration() {
        if (type == Operation.TYPE.MILLI) {
            return duration / 1000000;
        } else if (type == Operation.TYPE.MICRO) {
            return duration / 1000;
        } else {
            return duration;
        }
    }

    public String getName() {
        return name;
    }
}