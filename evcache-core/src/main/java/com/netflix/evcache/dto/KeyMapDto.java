package com.netflix.evcache.dto;

import com.netflix.evcache.EVCacheKey;
import java.util.Map;

public class KeyMapDto {
    Map<String, EVCacheKey> plainKeysMap;
    Map<String, EVCacheKey> hashedKeysMap;

    public KeyMapDto(Map<String, EVCacheKey> plainKeysMap, Map<String, EVCacheKey> hashedKeysMap) {
        this.plainKeysMap = plainKeysMap;
        this.hashedKeysMap = hashedKeysMap;
    }

    public Map<String, EVCacheKey> getPlainKeysMap() {
        return plainKeysMap;
    }

    public Map<String, EVCacheKey> getHashedKeysMap() {
        return hashedKeysMap;
    }
}
