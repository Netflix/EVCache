package com.netflix.evcache.util;

import com.netflix.evcache.EVCacheKey;

import java.util.List;
import java.util.Map;

public class EVCacheBulkDataDto<T> {
    private Map<String, T> decanonicalR;
    private List<EVCacheKey> evcKeys;

    public EVCacheBulkDataDto(Map<String, T> decanonicalR, List<EVCacheKey> evcKeys) {
        this.decanonicalR = decanonicalR;
        this.evcKeys = evcKeys;
    }

    public Map<String, T> getDecanonicalR() {
        return decanonicalR;
    }

    public List<EVCacheKey> getEvcKeys() {
        return evcKeys;
    }

    public void setDecanonicalR(Map<String, T> decanonicalR) {
        this.decanonicalR = decanonicalR;
    }

    public void setEvcKeys(List<EVCacheKey> evcKeys) {
        this.evcKeys = evcKeys;
    }
}
