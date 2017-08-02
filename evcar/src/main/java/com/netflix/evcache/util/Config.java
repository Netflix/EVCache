package com.netflix.evcache.util;

import com.netflix.config.NetflixConfiguration;

public class Config {

    private static boolean isMoneta = "moneta".equals(System.getenv("EVCACHE_SERVER_TYPE"));

    public static boolean isMoneta() {
        return isMoneta;
    }

    public static int getMemcachedPort() {
        if (isMoneta) {
            return NetflixConfiguration.getConfig().getInt("netflix.appinfo.metadata.udsproxy.memcached.port", -1);
        } else {
            return NetflixConfiguration.getConfig().getInt("netflix.appinfo.metadata.evcache.port", 11211);
        }
    }

}
