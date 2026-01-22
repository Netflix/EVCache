package com.netflix.evcache.config;

import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.archaius.config.PollingDynamicConfig;
import com.netflix.archaius.config.polling.FixedPollingStrategy;
import com.netflix.archaius.persisted2.DefaultPersisted2ClientConfig;
import com.netflix.archaius.persisted2.JsonPersistedV2Reader;
import com.netflix.archaius.persisted2.Persisted2ClientConfig;
import com.netflix.archaius.persisted2.ScopePredicates;
import com.netflix.archaius.persisted2.loader.HTTPStreamLoader;

public class EVCachePersistedProperties {
    private static Logger log = LoggerFactory.getLogger(EVCachePersistedProperties.class);

    private static final String SCOPE_CLUSTER    = "cluster";
    private static final String SCOPE_AMI        = "ami";
    private static final String SCOPE_ZONE       = "zone";
    private static final String SCOPE_ASG        = "asg";
    private static final String SCOPE_SERVER_ID  = "serverId";
    private static final String SCOPE_REGION     = "region";
    private static final String SCOPE_STACK      = "stack";
    private static final String SCOPE_ENV        = "env";
    private static final String SCOPE_APP_ID     = "appId";
    private PollingDynamicConfig config; 

    public EVCachePersistedProperties() {
    }

    private Persisted2ClientConfig getConfig() {

        final String region = System.getProperty("netflix.region", getSystemEnvValue("NETFLIX_REGION", "us-east-1"));
        final String env = System.getProperty("netflix.environment",       getSystemEnvValue("NETFLIX_ENVIRONMENT", "test"));
        String url = System.getProperty("platformserviceurl", String.format("http://platformservice.cluster.%s.%s.cloud.netflix.net:7001/platformservice/REST/v2/properties/jsonFilterprops", region, env));
        
        return new DefaultPersisted2ClientConfig()
                .setEnabled(true)
                .withServiceUrl(url)
                .withQueryScope(SCOPE_APP_ID, System.getProperty("netflix.appId",             getSystemEnvValue("NETFLIX_APP", "")), "")
                .withQueryScope(SCOPE_ENV,    env, "")
                .withQueryScope(SCOPE_STACK,  System.getProperty("netflix.stack",             getSystemEnvValue("NETFLIX_STACK", "")), "")
                .withQueryScope(SCOPE_REGION, region, "")

                .withScope(SCOPE_APP_ID,      System.getProperty("netflix.appId",             getSystemEnvValue("NETFLIX_APP", "")))
                .withScope(SCOPE_ENV,         env)
                .withScope(SCOPE_STACK,       System.getProperty("netflix.stack",             getSystemEnvValue("NETFLIX_STACK", "")))
                .withScope(SCOPE_REGION,      region)
                .withScope(SCOPE_SERVER_ID,   System.getProperty("netflix.serverId",          getSystemEnvValue("NETFLIX_INSTANCE_ID", "")))
                .withScope(SCOPE_ASG,         System.getProperty("netflix.appinfo.asgName",   getSystemEnvValue("NETFLIX_AUTO_SCALE_GROUP", "")))
                .withScope(SCOPE_ZONE,        getSystemEnvValue("EC2_AVAILABILITY_ZONE", ""))
                .withScope(SCOPE_AMI,         getSystemEnvValue("EC2_AMI_ID", ""))
                .withScope(SCOPE_CLUSTER,     getSystemEnvValue("NETFLIX_CLUSTER", ""))

                .withPrioritizedScopes(SCOPE_SERVER_ID, SCOPE_ASG, SCOPE_AMI, SCOPE_CLUSTER, SCOPE_APP_ID, SCOPE_ENV, SCOPE_STACK, SCOPE_ZONE, SCOPE_REGION)
                ;
    }

    private String getSystemEnvValue(String key, String def) {
        final String val = System.getenv(key);
        return val == null ? def : val;
    }

    private String getFilterString(Map<String, Set<String>> scopes) {
        StringBuilder sb = new StringBuilder();
        for (Entry<String, Set<String>> scope : scopes.entrySet()) {
            if (scope.getValue().isEmpty()) 
                continue;
            
            if (sb.length() > 0) {
                sb.append(" and ");
            }
            
            sb.append("(");
            
            boolean first = true;
            for (String value : scope.getValue()) {
                if (!first) {
                    sb.append(" or ");
                }
                else {
                    first = false;
                }
                sb.append(scope.getKey());
                if (null == value) {
                    sb.append(" is null");
                }
                else if (value.isEmpty()) {
                    sb.append("=''");
                }
                else {
                    sb.append("='").append(value).append("'");
                }
            }
            
            sb.append(")");
        }
        return sb.toString();
    }

    public PollingDynamicConfig getPollingDynamicConfig() {
        try {
            Persisted2ClientConfig clientConfig = getConfig();
            log.info("Remote config : " + clientConfig);
            String url = new StringBuilder()
                .append(clientConfig.getServiceUrl())
                .append("?skipPropsWithExtraScopes=").append(clientConfig.getSkipPropsWithExtraScopes())
                .append("&filter=").append(URLEncoder.encode(getFilterString(clientConfig.getQueryScopes()), "UTF-8"))
                .toString();

            if (clientConfig.isEnabled()) {
                JsonPersistedV2Reader reader = JsonPersistedV2Reader.builder(new HTTPStreamLoader(new URL(url)))
                        .withPath("propertiesList")
                        .withScopes(clientConfig.getPrioritizedScopes())
                        .withPredicate(ScopePredicates.fromMap(clientConfig.getScopes()))
                        .build();

                config = new PollingDynamicConfig(reader, new FixedPollingStrategy(clientConfig.getRefreshRate(), TimeUnit.SECONDS));
                return config;
            }
        } catch (Exception e1) {
            throw new RuntimeException(e1);
        }
        return null;
    }
}
