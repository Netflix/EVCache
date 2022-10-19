package com.netflix.evcache.dto;

import com.netflix.evcache.EVCacheKey;

import java.util.Map;

public class KeyMapDto {
    Map<String, EVCacheKey> keyMap;
    boolean isKeyHashed;

    public KeyMapDto(Map<String, EVCacheKey> keyMap, boolean isKeyHashed) {
        this.keyMap = keyMap;
        this.isKeyHashed = isKeyHashed;
    }

    public Map<String, EVCacheKey> getKeyMap() {
        return keyMap;
    }

    public boolean isKeyHashed() {
        return isKeyHashed;
    }
}
