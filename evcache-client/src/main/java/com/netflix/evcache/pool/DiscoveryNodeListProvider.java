package com.netflix.evcache.pool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.InetAddresses;
import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.config.ChainedDynamicProperty;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.shared.Application;
import com.netflix.evcache.util.EVCacheConfig;

public class DiscoveryNodeListProvider implements EVCacheNodeList {
    public static final String DEFAULT_PORT = "11211";

    private static Logger log = LoggerFactory.getLogger(DiscoveryNodeListProvider.class);
    private final DiscoveryClient _discoveryClient;
    private final String _appName;
    private final Map<ServerGroup, ChainedDynamicProperty.BooleanProperty> useLocalIpFPMap = new HashMap<ServerGroup, ChainedDynamicProperty.BooleanProperty>();
    private final ApplicationInfoManager applicationInfoManager;

    public DiscoveryNodeListProvider(ApplicationInfoManager applicationInfoManager, DiscoveryClient discoveryClient,
            String appName) {
        this.applicationInfoManager = applicationInfoManager;
        this._discoveryClient = discoveryClient;
        this._appName = appName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.evcache.pool.EVCacheNodeList#discoverInstances()
     */
    @Override
    public Map<ServerGroup, EVCacheServerGroupConfig> discoverInstances() throws IOException {

        if ((applicationInfoManager.getInfo().getStatus() == InstanceStatus.DOWN)) {
            return Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap();
        }

        /* Get a list of EVCACHE instances from the DiscoveryManager */
        final Application app = _discoveryClient.getApplication(_appName);
        if (app == null) return Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap();

        final List<InstanceInfo> appInstances = app.getInstances();
        final Map<ServerGroup, EVCacheServerGroupConfig> instancesSpecific = new HashMap<ServerGroup, EVCacheServerGroupConfig>();

        /* Iterate all the discovered instances to find usable ones */
        for (InstanceInfo iInfo : appInstances) {
            final DataCenterInfo dcInfo = iInfo.getDataCenterInfo();
            if (dcInfo == null) {
                if (log.isErrorEnabled()) log.error("Data Center Info is null for appName - " + _appName);
                continue;
            }

            /* Only AWS instances are usable; bypass all others */
            if (DataCenterInfo.Name.Amazon != dcInfo.getName()) {
                if (log.isErrorEnabled()) log.error(
                        "This is not a AWSDataCenter. You will not be able to use Discovery Nodelist Provider. Cannot proceed. DataCenterInfo : "
                                + dcInfo + "; appName - "
                                + _appName);
                continue;
            }

            final AmazonInfo amznInfo = (AmazonInfo) dcInfo; 
            // We checked above if this instance is Amazon so no need to do a instanceof check
            final String zone = amznInfo.get(AmazonInfo.MetaDataKey.availabilityZone);
            final String rSetName = iInfo.getASGName();
            final Map<String, String> metaInfo = iInfo.getMetadata();
            final int evcachePort = Integer.parseInt((metaInfo != null && metaInfo.containsKey("evcache.port")) ? metaInfo.get("evcache.port") : DEFAULT_PORT);
            final int rendPort = (metaInfo != null && metaInfo.containsKey("rend.port")) ? Integer.parseInt(metaInfo.get("rend.port")) : 0;
            final int udsproxyMemcachedPort = (metaInfo != null && metaInfo.containsKey("udsproxy.memcached.port")) ? Integer.parseInt(metaInfo.get("udsproxy.memcached.port")) : 0;
            final int udsproxyMementoPort = (metaInfo != null && metaInfo.containsKey("udsproxy.memento.port")) ? Integer.parseInt(metaInfo.get("udsproxy.memento.port")) : 0;
            final ServerGroup rSet = new ServerGroup(zone, rSetName);
            final Set<InetSocketAddress> instances;
            final EVCacheServerGroupConfig config;
            if (instancesSpecific.containsKey(rSet)) {
                config = instancesSpecific.get(rSet);
                instances = config.getInetSocketAddress();
            } else {
                instances = new HashSet<InetSocketAddress>();
                config = new EVCacheServerGroupConfig(rSet, instances, rendPort, udsproxyMemcachedPort, udsproxyMementoPort);
                instancesSpecific.put(rSet, config);
            }

            /* Don't try to use downed instances */
            final InstanceStatus status = iInfo.getStatus();
            if (status == null || InstanceStatus.OUT_OF_SERVICE == status || InstanceStatus.DOWN == status) {
                if (log.isDebugEnabled()) log.debug("The Status of the instance in Discovery is " + status + ". App Name : " + _appName + "; Zone : " + zone
                        + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
                continue;
            }

            
            ChainedDynamicProperty.BooleanProperty useLocalIp = useLocalIpFPMap.get(rSet);
            InetSocketAddress address = null;
            if (useLocalIp == null) {
                useLocalIp = EVCacheConfig.getInstance().getChainedBooleanProperty(_appName + "." + rSet.getName()+ ".use.localip", _appName + ".use.localip", Boolean.FALSE);
                useLocalIpFPMap.put(rSet, useLocalIp);
            }

            final InstanceInfo myInfo = applicationInfoManager.getInfo();
            final String myInstanceId = myInfo.getInstanceId();
            final String myIp = myInfo.getIPAddr();
            final String myPublicHostName = ((AmazonInfo) myInfo.getDataCenterInfo()).get(
                    AmazonInfo.MetaDataKey.publicHostname);
            boolean isInCloud = false;
            if (myPublicHostName != null) {
                isInCloud = myPublicHostName.startsWith("ec2");
            }

            if (!isInCloud) {
                if (((AmazonInfo) myInfo.getDataCenterInfo()).get(AmazonInfo.MetaDataKey.vpcId) != null) {
                    isInCloud = true;
                } else {
                    if (myIp.equals(myInstanceId)) {
                        isInCloud = false;
                    }
                }
            }
            final String myZone = ((AmazonInfo) myInfo.getDataCenterInfo()).get(AmazonInfo.MetaDataKey.availabilityZone);
            final String myRegion = (myZone != null) ? myZone.substring(0, myZone.length() - 1) : null;
            final String region = (zone != null) ? zone.substring(0, zone.length() - 1) : null;
            final String host = amznInfo.get(AmazonInfo.MetaDataKey.publicHostname);
            if (log.isDebugEnabled()) log.debug("myZone - " + myZone + "; zone : " + zone + "; myRegion : " + myRegion + "; region : " + region + "; host : " + host);
            if(host != null) {
                if (myRegion == null || region == null || !myRegion.equals(region)) {
                    // Hack so tests can work on desktop and in jenkins
                    final InetAddress inetAddress = InetAddress.getByName(host);
                    address = new InetSocketAddress(inetAddress, evcachePort);
                    if (log.isDebugEnabled()) log.debug("myZone - " + myZone + ". host : " + host
                            + "; inetAddress : " + inetAddress + "; address - " + address + "; App Name : " + _appName
                            + "; Zone : " + zone + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
                } else {
                    final String localIp = (isInCloud) ? amznInfo.get(AmazonInfo.MetaDataKey.localIpv4)
                            : amznInfo.get(AmazonInfo.MetaDataKey.publicIpv4);
                    final InetAddress add = InetAddresses.forString(localIp);
                    final InetAddress inetAddress = InetAddress.getByAddress(host, add.getAddress());
                    address = new InetSocketAddress(inetAddress, evcachePort);
                    if (log.isDebugEnabled()) log.debug("CLASSIC : localIp - " + localIp + ". host : " + host + "; add : "
                            + add + "; inetAddress : " + inetAddress + "; address - " + address + "; App Name : " + _appName
                            + "; Zone : " + zone + "; myZone - " + myZone + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
                }
            } else {
                if (useLocalIp.get().booleanValue() || amznInfo.get(AmazonInfo.MetaDataKey.vpcId) != null) {
                    final String localIp = amznInfo.get(AmazonInfo.MetaDataKey.localIpv4);

                    final InetAddress add = InetAddresses.forString(localIp);
                    final InetAddress inetAddress = InetAddress.getByAddress(localIp, add.getAddress());
                    address = new InetSocketAddress(inetAddress, evcachePort);
    
                    if (log.isDebugEnabled()) log.debug("VPC : localIp - " + localIp + " ; add : " + add + "; inetAddress : " + inetAddress + "; address - " + address 
                            + "; App Name : " + _appName + "; Zone : " + zone + "; myZone - " + myZone + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
                } else {
                    final String localIp = (isInCloud) ? amznInfo.get(AmazonInfo.MetaDataKey.localIpv4)
                            : amznInfo.get(AmazonInfo.MetaDataKey.publicIpv4);
                    final InetAddress add = InetAddresses.forString(localIp);
                    final InetAddress inetAddress = InetAddress.getByAddress(localIp, add.getAddress());
                    address = new InetSocketAddress(inetAddress, evcachePort);
                    if (log.isDebugEnabled()) log.debug("CLASSIC : localIp - " + localIp + " ; add : " + add + "; inetAddress : " + inetAddress + "; address - " + address 
                            + "; App Name : " + _appName + "; Zone : " + zone + "; myZone - " + myZone + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
                }
            }

            instances.add(address);
        }
        return instancesSpecific;
    }
}
