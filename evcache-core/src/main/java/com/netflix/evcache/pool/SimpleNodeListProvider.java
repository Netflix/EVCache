package com.netflix.evcache.pool;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.netflix.archaius.api.PropertyRepository;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.InetAddresses;
import com.netflix.archaius.api.Property;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.Tag;
import com.netflix.evcache.pool.EVCacheClientPool;

public class SimpleNodeListProvider implements EVCacheNodeList {

    private static final Logger log = LoggerFactory.getLogger(EVCacheClientPool.class);
    private static final String EUREKA_TIMEOUT = "evcache.eureka.timeout";

    private String currentNodeList = "";
    private final int timeout;
    private String region = null;
    private String env = null;

    public SimpleNodeListProvider() {
        
        final String timeoutStr = System.getProperty(EUREKA_TIMEOUT);
        this.timeout = (timeoutStr != null) ? Integer.parseInt(timeoutStr) : 5000;

        final String sysEnv = System.getenv("NETFLIX_ENVIRONMENT");
        if(sysEnv != null)  {
            env = sysEnv;
        } else {
            String propEnv = null;
            if(propEnv == null) propEnv = System.getProperty("@environment");
            if(propEnv == null) propEnv = System.getProperty("eureka.environment");
            if(propEnv == null) propEnv = System.getProperty("netflix.environment");
            env = propEnv;
        }

        final String sysRegion = System.getenv("EC2_REGION");
        if(sysRegion != null)  {
            region = sysRegion;
        } else {
            String propRegion = null;
            if(propRegion == null) propRegion = System.getProperty("@region");
            if(propRegion == null) propRegion = System.getProperty("eureka.region");
            if(propRegion == null) propRegion = System.getProperty("netflix.region");
            region = propRegion;
        }

    }

    /**
     * Pass a System Property of format
     * 
     * <EVCACHE_APP>-NODES=setname0=instance01:port,instance02:port,
     * instance03:port;setname1=instance11:port,instance12:port,instance13:port;
     * setname2=instance21:port,instance22:port,instance23:port
     * 
     */
    @Override
    public Map<ServerGroup, EVCacheServerGroupConfig> discoverInstances(String appName) throws IOException {
        final String propertyName = appName + "-NODES";
        final String nodeListString = EVCacheConfig.getInstance().getPropertyRepository().get(propertyName, String.class).orElse("").get();
        if (log.isDebugEnabled()) log.debug("List of Nodes = " + nodeListString);
        if(nodeListString != null && nodeListString.length() > 0) return bootstrapFromSystemProperty(nodeListString);
        
        if(env != null && region != null) return bootstrapFromEureka(appName);
        
        return Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap();
    }

    /**
     * Netflix specific impl so we can load from eureka. 
     * @param appName
     * @return
     * @throws IOException
     */
    private Map<ServerGroup, EVCacheServerGroupConfig> bootstrapFromEureka(String appName) throws IOException {
        
        if(env == null || region == null) return Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap();

        final String url = "http://discoveryreadonly." + region + ".dyn" + env + ".netflix.net:7001/v2/apps/" + appName;
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final long start = System.currentTimeMillis();
        PropertyRepository props = EVCacheConfig.getInstance().getPropertyRepository();
        CloseableHttpResponse httpResponse = null;
        try {
            final RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).build();
            HttpGet httpGet = new HttpGet(url);
            httpGet.addHeader("Accept", "application/json");
            httpGet.setConfig(requestConfig);
            httpResponse = httpclient.execute(httpGet);
            final int statusCode = httpResponse.getStatusLine().getStatusCode();
            if (!(statusCode >= 200 && statusCode < 300)) {
                log.error("Status Code : " + statusCode + " for url " + url);
                return Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap();
            }
            
            final InputStreamReader in = new InputStreamReader(httpResponse.getEntity().getContent(), Charset.defaultCharset());
            final JSONTokener js = new JSONTokener(in);
            final JSONObject jsonObj = new JSONObject(js);
            final JSONObject application = jsonObj.getJSONObject("application");
            final JSONArray instances = application.getJSONArray("instance");
            final Map<ServerGroup, EVCacheServerGroupConfig> serverGroupMap = new HashMap<ServerGroup, EVCacheServerGroupConfig>();
            final int securePort = Integer.parseInt(props.get("evcache.secure.port", String.class)
                    .orElse(EVCacheClientPool.DEFAULT_SECURE_PORT).get());

            for(int i = 0; i < instances.length(); i++) {
                final JSONObject instanceObj = instances.getJSONObject(i);
                final JSONObject metadataObj = instanceObj.getJSONObject("dataCenterInfo").getJSONObject("metadata");

                final String asgName = instanceObj.getString("asgName");
                final Property<Boolean> asgEnabled = props.get(asgName + ".enabled", Boolean.class).orElse(true);
                final boolean isSecure = props.get(asgName + ".use.secure", Boolean.class)
                        .orElseGet(appName + ".use.secure")
                        .orElseGet("evcache.use.secure")
                        .orElse(false).get();

                if (!asgEnabled.get()) {
                    if(log.isDebugEnabled()) log.debug("ASG " + asgName + " is disabled so ignoring it");
                    continue;
                }
                
                final String zone = metadataObj.getString("availability-zone");
                final ServerGroup rSet = new ServerGroup(zone, asgName);
                final String localIp = metadataObj.getString("local-ipv4");
                final JSONObject instanceMetadataObj = instanceObj.getJSONObject("metadata");
                final String evcachePortString = instanceMetadataObj.optString("evcache.port",
                        EVCacheClientPool.DEFAULT_PORT);
                final int evcachePort = Integer.parseInt(evcachePortString);
                final int port = isSecure ? securePort : evcachePort;

                EVCacheServerGroupConfig config = serverGroupMap.get(rSet);
                if(config == null) {
                    config = new EVCacheServerGroupConfig(rSet, new HashSet<InetSocketAddress>());
                    serverGroupMap.put(rSet, config);
//                    final ArrayList<Tag> tags = new ArrayList<Tag>(2);
//                    tags.add(new BasicTag(EVCacheMetricsFactory.CACHE, appName));
//                    tags.add(new BasicTag(EVCacheMetricsFactory.SERVERGROUP, rSet.getName()));
//                    EVCacheMetricsFactory.getInstance().getLongGauge(EVCacheMetricsFactory.CONFIG, tags).set(Long.valueOf(port));
                }

                final InetAddress add = InetAddresses.forString(localIp);
                final InetAddress inetAddress = InetAddress.getByAddress(localIp, add.getAddress());
                final InetSocketAddress address = new InetSocketAddress(inetAddress, port);
                config.getInetSocketAddress().add(address);
            }
            if (log.isDebugEnabled()) log.debug("Returning : " + serverGroupMap);
            return serverGroupMap;
        } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug("URL : " + url + "; Timeout " + timeout, e);
        } finally {
            if (httpResponse != null) {
                try {
                    httpResponse.close();
                } catch (IOException e) {

                }
            }
            final List<Tag> tagList = new ArrayList<Tag>(2);
            EVCacheMetricsFactory.getInstance().addAppNameTags(tagList, appName);
            if (log.isDebugEnabled()) log.debug("Total Time to execute " + url + " " + (System.currentTimeMillis() - start) + " msec.");
            EVCacheMetricsFactory.getInstance().getPercentileTimer(EVCacheMetricsFactory.INTERNAL_BOOTSTRAP_EUREKA, tagList, Duration.ofMillis(100)).record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        }
        return Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap();
    }
    
    
    private Map<ServerGroup, EVCacheServerGroupConfig> bootstrapFromSystemProperty(String nodeListString ) throws IOException {
        final Map<ServerGroup, EVCacheServerGroupConfig> instancesSpecific = new HashMap<ServerGroup,EVCacheServerGroupConfig>();
        final StringTokenizer setTokenizer = new StringTokenizer(nodeListString, ";");
        while (setTokenizer.hasMoreTokens()) {
            final String token = setTokenizer.nextToken();
            final StringTokenizer replicaSetTokenizer = new StringTokenizer(token, "=");
            while (replicaSetTokenizer.hasMoreTokens()) {
                final String replicaSetToken = replicaSetTokenizer.nextToken();
                final String instanceToken = replicaSetTokenizer.nextToken();
                final StringTokenizer instanceTokenizer = new StringTokenizer(instanceToken, ",");
                final Set<InetSocketAddress> instanceList = new HashSet<InetSocketAddress>();
                final ServerGroup rSet = new ServerGroup(replicaSetToken, replicaSetToken);
                final EVCacheServerGroupConfig config = new EVCacheServerGroupConfig(rSet, instanceList);
                instancesSpecific.put(rSet, config);
                while (instanceTokenizer.hasMoreTokens()) {
                    final String instance = instanceTokenizer.nextToken();
                    int index = instance.indexOf(':');
                    String host = instance.substring(0, index);
                    String port = instance.substring(index + 1);
                    int ind = host.indexOf('/');
                    if (ind == -1) {
                        final InetAddress add = InetAddress.getByName(host);
                        instanceList.add(new InetSocketAddress(add, Integer.parseInt(port)));
                    } else {
                        final String hostName = host.substring(0, ind);
                        final String localIp = host.substring(ind + 1);
                        final InetAddress add = InetAddresses.forString(localIp);
                        final InetAddress inetAddress = InetAddress.getByAddress(hostName, add.getAddress());
                        instanceList.add(new InetSocketAddress(inetAddress, Integer.parseInt(port)));
                    }
                }
            }
        }

        currentNodeList = nodeListString;
        if(log.isDebugEnabled()) log.debug("List by Servergroup" + instancesSpecific);
        return instancesSpecific;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"Current Node List\":\"");
        builder.append(currentNodeList);
        builder.append("\"");
        builder.append("\"}");
        return builder.toString();
    }

}
