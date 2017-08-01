package com.netflix.evcache.server.keydump;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MnemonicInfo {

    @JsonProperty(value="MnemonicConfigPath")
    private String mnemonicConfigPath;

    @JsonProperty(value="DbPaths")
    private List<String> dbPaths;

    public MnemonicInfo() {
    }

    public MnemonicInfo(String configPath, List<String> dbPaths) {
        this.mnemonicConfigPath = configPath;
        this.dbPaths = Collections.unmodifiableList(dbPaths);
    }

    public String getMnemonicConfigPath() {
        return mnemonicConfigPath;
    }

    public List<String> getDbPaths() {
        return dbPaths;
    }
}
