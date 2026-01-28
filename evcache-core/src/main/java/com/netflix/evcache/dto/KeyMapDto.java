package com.netflix.evcache.dto;

import com.netflix.evcache.EVCacheKey;
import java.util.Map;

public class KeyMapDto {
    Map<String, EVCacheKey> allKeysMap;
    Map<String, EVCacheKey> hashedKeysMap;

    public KeyMapDto(Map<String, EVCacheKey> allKeysMap, Map<String, EVCacheKey> hashedKeysMap) {
        this.allKeysMap = allKeysMap;
        this.hashedKeysMap = hashedKeysMap;
    }

    public Map<String, EVCacheKey> getAllKeysMap() {
        return allKeysMap;
    }

    public Map<String, EVCacheKey> getHashedKeysMap() {
        return hashedKeysMap;
    }

    public boolean hasHashedKeys() {
        return !hashedKeysMap.isEmpty();
    }
}
