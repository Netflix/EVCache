package com.netflix.evcache.pool;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

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
import com.netflix.config.ChainedDynamicProperty;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.servo.tag.BasicTagList;

public class SimpleNodeListProvider implements EVCacheNodeList {

    private static Logger log = LoggerFactory.getLogger(EVCacheClientPool.class);
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
            String propEnv = System.getProperty("NETFLIX_ENVIRONMENT");
            if(propEnv == null) propEnv = System.getProperty("@environment");
            if(propEnv == null) propEnv = System.getProperty("eureka.environment");
            env = propEnv;
        }

        final String sysRegion = System.getenv("EC2_REGION");
        if(sysRegion != null)  {
            region = sysRegion;
        } else {
            String propRegion = System.getProperty("EC2_REGION");
            if(propRegion == null) propRegion = System.getProperty("@region");
            if(propRegion == null) propRegion = System.getProperty("eureka.region");
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
        final String nodeListString = System.getProperty(propertyName);
        if (log.isDebugEnabled()) log.debug("List of Nodes = " + nodeListString);
        if(nodeListString != null && nodeListString.length() > 0) return bootstrapFromSystemProperty(nodeListString);
        
        if(env != null && region != null) return bootstrapFromEureka(appName);
        
        return Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap();
    }

    private Map<ServerGroup, EVCacheServerGroupConfig> bootstrapFromEureka(String appName) throws IOException {
        
        if(env == null || region == null) return Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap();
        
        final String url = "http://discoveryreadonly." + region + ".dyn" + env + ".netflix.net:7001/v2/apps/" + appName;
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final long start = System.currentTimeMillis();
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
            final ChainedDynamicProperty.BooleanProperty useBatchPort = EVCacheConfig.getInstance().getChainedBooleanProperty(appName + ".use.batch.port", "evcache.use.batch.port", Boolean.FALSE);
            for(int i = 0; i < instances.length(); i++) {
                final JSONObject instanceObj = instances.getJSONObject(i);
                final JSONObject metadataObj = instanceObj.getJSONObject("dataCenterInfo").getJSONObject("metadata");

                final ServerGroup rSet = new ServerGroup(metadataObj.getString("availability-zone"), instanceObj.getString("asgName"));
                final String localIp = metadataObj.getString("local-ipv4");
                final JSONObject instanceMetadataObj = instanceObj.getJSONObject("metadata");
                final String evcachePortString = instanceMetadataObj.optString("evcache.port", "11211");
                final String rendPortString = instanceMetadataObj.optString("rend.port", "0");
                final String rendBatchPortString = instanceMetadataObj.optString("rend.batch.port", "0");
                final int rendPort = Integer.parseInt(rendPortString);
                final int rendBatchPort = Integer.parseInt(rendBatchPortString);
                final String rendMemcachedPortString = instanceMetadataObj.optString("rend.memcached.port", "0");
                final String rendMementoPortString = instanceMetadataObj.optString("rend.memento.port", "0");
                final int evcachePort = Integer.parseInt(evcachePortString);
                final int port = rendPort == 0 ? evcachePort : ((useBatchPort.get().booleanValue()) ? rendBatchPort : rendPort);

                EVCacheServerGroupConfig config = serverGroupMap.get(rSet);
                if(config == null) {

                    config = new EVCacheServerGroupConfig(rSet, new HashSet<InetSocketAddress>(), Integer.parseInt(rendPortString), 
                            Integer.parseInt(rendMemcachedPortString), Integer.parseInt(rendMementoPortString));
                    serverGroupMap.put(rSet, config);
                    EVCacheMetricsFactory.getLongGauge(appName + "-port", BasicTagList.of("ServerGroup", rSet.getName(), "APP", appName)).set(Long.valueOf(port));
                }
                
                final InetAddress add = InetAddresses.forString(localIp);
                final InetAddress inetAddress = InetAddress.getByAddress(localIp, add.getAddress());
                final InetSocketAddress address = new InetSocketAddress(inetAddress, port);
                config.getInetSocketAddress().add(address);
            }
            if (log.isDebugEnabled()) log.debug("Returining : " + serverGroupMap);
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
            if (log.isDebugEnabled()) log.debug("Total Time to execute " + url + " " + (System.currentTimeMillis() - start) + " msec.");
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
                final EVCacheServerGroupConfig config = new EVCacheServerGroupConfig(rSet, instanceList, 0, 0, 0);
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
