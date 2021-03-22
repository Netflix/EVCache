package com.netflix.evcache.pool.eureka;

import com.google.common.net.InetAddresses;
import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.EVCacheNodeList;
import com.netflix.evcache.pool.EVCacheServerGroupConfig;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EurekaNodeListProvider implements EVCacheNodeList {
    public static final String DEFAULT_PORT = "11211";
    public static final String DEFAULT_SECURE_PORT = "11443";

    private static final Logger log = LoggerFactory.getLogger(EurekaNodeListProvider.class);
    private final EurekaClient _eurekaClient;
    private PropertyRepository props;
    private final ApplicationInfoManager applicationInfoManager;
    private final Map<String, Property<Boolean>> useRendBatchPortMap = new HashMap<String, Property<Boolean>>();
    @SuppressWarnings("rawtypes") // Archaius2 PropertyRepository does not support ParameterizedTypes
	private Property<Set> ignoreHosts = null;

    public EurekaNodeListProvider(ApplicationInfoManager applicationInfoManager, EurekaClient eurekaClient, PropertyRepository props) {
        this.applicationInfoManager = applicationInfoManager;
        this._eurekaClient = eurekaClient;
        this.props = props;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.netflix.evcache.pool.EVCacheNodeList#discoverInstances()
     */
    @Override
    public Map<ServerGroup, EVCacheServerGroupConfig> discoverInstances(String _appName) throws IOException {
        final Property<Boolean> ignoreAppEurekaStatus = props.get("evcache.ignoreAppEurekaStatus", Boolean.class).orElse(false);

        if (ignoreAppEurekaStatus.get())
            log.info("Not going to consider the eureka status of the application, to initialize evcache client.");

        if (!ignoreAppEurekaStatus.get() && (applicationInfoManager.getInfo().getStatus() == InstanceStatus.DOWN)) {
            log.info("Not initializing evcache client as application eureka status is DOWN. " +
                    "One can override this behavior by setting evcache.ignoreAppEurekaStatus on application.");
            return Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap();
        }

        /* Get a list of EVCACHE instances from the DiscoveryManager */
        final Application app = _eurekaClient.getApplication(_appName);
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
            if (DataCenterInfo.Name.Amazon != dcInfo.getName() || !(dcInfo instanceof AmazonInfo)) {
                log.error("This is not an AWSDataCenter. You will not be able to use Discovery Nodelist Provider. Cannot proceed. " +
                          "DataCenterInfo : {}; appName - {}. Please use SimpleNodeList provider and specify the server groups manually.",
                          dcInfo, _appName);
                continue;
            }

            final AmazonInfo amznInfo = (AmazonInfo) dcInfo;
            // We checked above if this instance is Amazon so no need to do a instanceof check
            final String zone = amznInfo.get(AmazonInfo.MetaDataKey.availabilityZone);
            if(zone == null) {
                final List<Tag> tagList = new ArrayList<Tag>(3);
                EVCacheMetricsFactory.getInstance().addAppNameTags(tagList, _appName);
                tagList.add(new BasicTag(EVCacheMetricsFactory.CONFIG_NAME, EVCacheMetricsFactory.NULL_ZONE));
                EVCacheMetricsFactory.getInstance().increment(EVCacheMetricsFactory.CONFIG, tagList);
                continue;
            }
            final String asgName = iInfo.getASGName();
            if(asgName == null) {
                final List<Tag> tagList = new ArrayList<Tag>(3);
                EVCacheMetricsFactory.getInstance().addAppNameTags(tagList, _appName);
                tagList.add(new BasicTag(EVCacheMetricsFactory.CONFIG_NAME, EVCacheMetricsFactory.NULL_SERVERGROUP));
                EVCacheMetricsFactory.getInstance().increment(EVCacheMetricsFactory.CONFIG, tagList);
                continue;
            }

            final Property<Boolean> asgEnabled = props.get(asgName + ".enabled", Boolean.class).orElse(true);
            if (!asgEnabled.get()) {
                if(log.isDebugEnabled()) log.debug("ASG " + asgName + " is disabled so ignoring it");
                continue;
            }

            final Map<String, String> metaInfo = iInfo.getMetadata();
            final int evcachePort = Integer.parseInt((metaInfo != null && metaInfo.containsKey("evcache.port")) ? metaInfo.get("evcache.port") : DEFAULT_PORT);
            final int rendPort = (metaInfo != null && metaInfo.containsKey("rend.port")) ? Integer.parseInt(metaInfo.get("rend.port")) : 0;
            final int rendBatchPort = (metaInfo != null && metaInfo.containsKey("rend.batch.port")) ? Integer.parseInt(metaInfo.get("rend.batch.port")) : 0;
            final int udsproxyMemcachedPort = (metaInfo != null && metaInfo.containsKey("udsproxy.memcached.port")) ? Integer.parseInt(metaInfo.get("udsproxy.memcached.port")) : 0;
            final int udsproxyMementoPort = (metaInfo != null && metaInfo.containsKey("udsproxy.memento.port")) ? Integer.parseInt(metaInfo.get("udsproxy.memento.port")) : 0;

            Property<Boolean> useBatchPort = useRendBatchPortMap.get(asgName);
            if (useBatchPort == null) {
                useBatchPort = props.get(_appName + ".use.batch.port", Boolean.class).orElseGet("evcache.use.batch.port").orElse(false);
                useRendBatchPortMap.put(asgName, useBatchPort);
            }
            int port = rendPort == 0 ? evcachePort : ((useBatchPort.get().booleanValue()) ? rendBatchPort : rendPort);
            final Property<Boolean> isSecure = props.get(asgName + ".use.secure", Boolean.class).orElseGet(_appName + ".use.secure").orElse(false);
            if(isSecure.get()) {
                port = Integer.parseInt((metaInfo != null && metaInfo.containsKey("evcache.secure.port")) ? metaInfo.get("evcache.secure.port") : DEFAULT_SECURE_PORT);
            }

            final ServerGroup serverGroup = new ServerGroup(zone, asgName);
            final Set<InetSocketAddress> instances;
            final EVCacheServerGroupConfig config;
            if (instancesSpecific.containsKey(serverGroup)) {
                config = instancesSpecific.get(serverGroup);
                instances = config.getInetSocketAddress();
            } else {
                instances = new HashSet<InetSocketAddress>();
                config = new EVCacheServerGroupConfig(serverGroup, instances, rendPort, udsproxyMemcachedPort, udsproxyMementoPort);
                instancesSpecific.put(serverGroup, config);
                //EVCacheMetricsFactory.getInstance().getRegistry().gauge(EVCacheMetricsFactory.getInstance().getRegistry().createId(_appName + "-port", "ServerGroup", asgName, "APP", _appName), Long.valueOf(port));
            }

            /* Don't try to use downed instances */
            final InstanceStatus status = iInfo.getStatus();
            if (status == null || InstanceStatus.OUT_OF_SERVICE == status || InstanceStatus.DOWN == status) {
                if (log.isDebugEnabled()) log.debug("The Status of the instance in Discovery is " + status + ". App Name : " + _appName + "; Zone : " + zone
                        + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
                continue;
            }

            final InstanceInfo myInfo = applicationInfoManager.getInfo();
            final DataCenterInfo myDC = myInfo.getDataCenterInfo();
            final AmazonInfo myAmznDC = (myDC instanceof AmazonInfo) ? (AmazonInfo) myDC : null;
            final String myInstanceId = myInfo.getInstanceId();
            final String myIp = myInfo.getIPAddr();
            final String myPublicHostName = (myAmznDC != null) ? myAmznDC.get(AmazonInfo.MetaDataKey.publicHostname) : null;
            boolean isInCloud = false;
            if (myPublicHostName != null) {
                isInCloud = myPublicHostName.startsWith("ec2");
            }

            if (!isInCloud) {
                if (myAmznDC != null && myAmznDC.get(AmazonInfo.MetaDataKey.vpcId) != null) {
                    isInCloud = true;
                } else {
                    if (myIp.equals(myInstanceId)) {
                        isInCloud = false;
                    }
                }
            }
            final String myZone = (myAmznDC != null) ? myAmznDC.get(AmazonInfo.MetaDataKey.availabilityZone) : null;
            final String myRegion = (myZone != null) ? myZone.substring(0, myZone.length() - 1) : null;
            final String region = (zone != null) ? zone.substring(0, zone.length() - 1) : null;
            final String host = amznInfo.get(AmazonInfo.MetaDataKey.publicHostname);
            InetSocketAddress address = null;
            final String vpcId = amznInfo.get(AmazonInfo.MetaDataKey.vpcId);
            final String localIp = amznInfo.get(AmazonInfo.MetaDataKey.localIpv4);
            if (log.isDebugEnabled()) log.debug("myZone - " + myZone + "; zone : " + zone + "; myRegion : " + myRegion + "; region : " + region + "; host : " + host + "; vpcId : " + vpcId);

            if(ignoreHosts == null) ignoreHosts = props.get(_appName + ".ignore.hosts", Set.class).orElse(Collections.emptySet());
            if(localIp != null && ignoreHosts.get().contains(localIp)) continue;
            if(host != null && ignoreHosts.get().contains(host)) continue;

            if (vpcId != null) {
                final InetAddress add = InetAddresses.forString(localIp);
                final InetAddress inetAddress = InetAddress.getByAddress(localIp, add.getAddress());
                address = new InetSocketAddress(inetAddress, port);

                if (log.isDebugEnabled()) log.debug("VPC : localIp - " + localIp + " ; add : " + add + "; inetAddress : " + inetAddress + "; address - " + address
                        + "; App Name : " + _appName + "; Zone : " + zone + "; myZone - " + myZone + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
            } else {
                if(host != null && host.startsWith("ec2")) {
                    final InetAddress inetAddress = (localIp != null) ? InetAddress.getByAddress(host, InetAddresses.forString(localIp).getAddress()) : InetAddress.getByName(host);
                    address = new InetSocketAddress(inetAddress, port);
                    if (log.isDebugEnabled()) log.debug("myZone - " + myZone + ". host : " + host
                            + "; inetAddress : " + inetAddress + "; address - " + address + "; App Name : " + _appName
                            + "; Zone : " + zone + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
                } else {
                    final String ipToUse = (isInCloud) ? localIp : amznInfo.get(AmazonInfo.MetaDataKey.publicIpv4);
                    final InetAddress add = InetAddresses.forString(ipToUse);
                    final InetAddress inetAddress = InetAddress.getByAddress(ipToUse, add.getAddress());
                    address = new InetSocketAddress(inetAddress, port);
                    if (log.isDebugEnabled()) log.debug("CLASSIC : IPToUse - " + ipToUse + " ; add : " + add + "; inetAddress : " + inetAddress + "; address - " + address
                            + "; App Name : " + _appName + "; Zone : " + zone + "; myZone - " + myZone + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
                }
            }

            instances.add(address);
        }
        return instancesSpecific;
    }
}
