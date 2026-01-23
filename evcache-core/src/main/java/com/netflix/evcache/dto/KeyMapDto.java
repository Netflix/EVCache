package com.netflix.evcache.dto;

import com.netflix.evcache.EVCacheKey;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public class KeyMapDto {
    Map<String, EVCacheKey> hashedKeyMap;
    Map<String, EVCacheKey> nonHashedKeyMap;

    public KeyMapDto(Map<String, EVCacheKey> hashedKeyMap, Map<String, EVCacheKey> nonHashedKeyMap) {
        this.hashedKeyMap = hashedKeyMap;
        this.nonHashedKeyMap = nonHashedKeyMap;
    }

    public Set<String> getAllKeys() {
        Set<String> allKeys = new HashSet<>(hashedKeyMap.size() + nonHashedKeyMap.size());
        allKeys.addAll(hashedKeyMap.keySet());
        allKeys.addAll(nonHashedKeyMap.keySet());
        return allKeys;
    }

    public Map<String, EVCacheKey> getHashedKeyMap() {
        return hashedKeyMap;
    }

    public Map<String, EVCacheKey> getNonHashedKeyMap() {
        return nonHashedKeyMap;
    }
}
