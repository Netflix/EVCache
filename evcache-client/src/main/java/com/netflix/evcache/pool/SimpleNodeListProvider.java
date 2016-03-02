package com.netflix.evcache.pool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.InetAddresses;

public class SimpleNodeListProvider implements EVCacheNodeList {

    private static Logger log = LoggerFactory.getLogger(EVCacheClientPool.class);

    private String currentNodeList = "";
    private final String propertyName;

    public SimpleNodeListProvider(String propertyName) {
        this.propertyName = propertyName;
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
    public Map<ServerGroup, EVCacheServerGroupConfig> discoverInstances() throws IOException {
        final String nodeListString = System.getProperty(propertyName);
        if (log.isDebugEnabled())
            log.debug("List of Nodes" + nodeListString);

        if (nodeListString != null && nodeListString.length() > 0) {
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
            log.debug("List by Servergroup" + instancesSpecific);
            return instancesSpecific;
        }

        return Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"Current Node List\":\"");
        builder.append(currentNodeList);
        builder.append("\",\"System Property Name\":\"");
        builder.append(propertyName);
        builder.append("\"}");
        return builder.toString();
    }

}
