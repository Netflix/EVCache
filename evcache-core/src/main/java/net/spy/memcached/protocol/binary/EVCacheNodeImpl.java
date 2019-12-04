package net.spy.memcached.protocol.binary;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.EVCache;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Tag;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.EVCacheNode;
import net.spy.memcached.EVCacheNodeMBean;
import net.spy.memcached.ops.Operation;
//import sun.misc.Cleaner;
//import sun.nio.ch.DirectBuffer;

@SuppressWarnings("restriction")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({ "FCBL_FIELD_COULD_BE_LOCAL", "EXS_EXCEPTION_SOFTENING_NO_CHECKED",
        "REC_CATCH_EXCEPTION", "SCII_SPOILED_CHILD_INTERFACE_IMPLEMENTATOR" })
public class EVCacheNodeImpl extends BinaryMemcachedNodeImpl implements EVCacheNodeMBean, EVCacheNode {
    private static final Logger log = LoggerFactory.getLogger(EVCacheNodeImpl.class);

    protected long stTime;
    protected final String hostName;
    protected final BlockingQueue<Operation> readQ;
    protected final BlockingQueue<Operation> inputQueue;
    protected final EVCacheClient client;
    //protected Counter reconnectCounter;
    private final AtomicInteger numOps = new AtomicInteger(0);
    private long timeoutStartTime;
    protected final Counter operationsCounter;

    public EVCacheNodeImpl(SocketAddress sa, SocketChannel c, int bufSize, BlockingQueue<Operation> rq, BlockingQueue<Operation> wq, BlockingQueue<Operation> iq,
            long opQueueMaxBlockTimeMillis, boolean waitForAuth, long dt, long at, ConnectionFactory fa, EVCacheClient client, long stTime) {
        super(sa, c, bufSize, rq, wq, iq, Long.valueOf(opQueueMaxBlockTimeMillis), waitForAuth, dt, at, fa);

        this.client = client;
        final String appName = client.getAppName();
        this.readQ = rq;
        this.inputQueue = iq;
        this.hostName = ((InetSocketAddress) getSocketAddress()).getHostName();
//        final List<Tag> tagsCounter = new ArrayList<Tag>(5);
//        tagsCounter.add(new BasicTag(EVCacheMetricsFactory.CACHE, client.getAppName()));
//        tagsCounter.add(new BasicTag(EVCacheMetricsFactory.SERVERGROUP, client.getServerGroupName()));
//        tagsCounter.add(new BasicTag(EVCacheMetricsFactory.ZONE, client.getZone()));
        //tagsCounter.add(new BasicTag(EVCacheMetricsFactory.HOST, hostName)); //TODO : enable this and see what is the impact
        this.operationsCounter = client.getOperationCounter();

        setConnectTime(stTime);
        setupMonitoring(appName);
    }

    private String getMonitorName(String appName) {
        return "com.netflix.evcache:Group=" + appName + ",SubGroup=pool" + ",SubSubGroup=" + client.getServerGroupName()
                + ",SubSubSubGroup=" + client.getId() + ",SubSubSubSubGroup=" + hostName
                + "_" + stTime;
    }

    private void setupMonitoring(String appName) {
        try {
            final ObjectName mBeanName = ObjectName.getInstance(getMonitorName(appName));
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mbeanServer.isRegistered(mBeanName)) {
                if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanName + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanName);
            }
            mbeanServer.registerMBean(this, mBeanName);
        } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug("Exception while setting up the monitoring.", e);
        }
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#registerMonitors()
     */
    @Override
    public void registerMonitors() {
//        try {
//            EVCacheMetricsFactory.getInstance().getRegistry().register(this);
//        } catch (Exception e) {
//            if (log.isWarnEnabled()) log.warn("Exception while registering.", e);
//        }
    }


    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#isAvailable(com.netflix.evcache.EVCache.Call)
     */
    @Override
    public boolean isAvailable(EVCache.Call call) {
        return isActive();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getWriteQueueSize()
     */
    @Override
    public int getWriteQueueSize() {
        return writeQ.size();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getReadQueueSize()
     */
    @Override
    public int getReadQueueSize() {
        return readQ.size();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getInputQueueSize()
     */
    @Override
    public int getInputQueueSize() {
        return inputQueue.size();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#incrOps()
     */
    @Override
    public long incrOps() {
        operationsCounter.increment();
        return numOps.incrementAndGet();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getNumOfOps()
     */
    @Override
    public long getNumOfOps() {
        return numOps.get();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#flushInputQueue()
     */
    @Override
    public void flushInputQueue() {
        inputQueue.clear();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getStartTime()
     */
    @Override
    public long getStartTime() {
        return stTime;
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getTimeoutStartTime()
     */
    @Override
    public long getTimeoutStartTime() {
        return timeoutStartTime;
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#removeMonitoring()
     */
    @Override
    public void removeMonitoring() {
        try {
            final ObjectName mBeanName = ObjectName.getInstance(getMonitorName(client.getAppName()));
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mbeanServer.isRegistered(mBeanName)) {
                if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanName + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanName);
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug("Exception while setting up the monitoring.", e);
        }
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#shutdown()
     */
    @Override
    public void shutdown() {
        removeMonitoring();
        writeQ.clear();
        readQ.clear();
        inputQueue.clear();
        try {
            // Cleanup the ByteBuffers only if they are sun.nio.ch.DirectBuffer
            // If we don't cleanup then we will leak 16K of memory
//            if (getRbuf() instanceof DirectBuffer) {
//                Cleaner cleaner = ((DirectBuffer) getRbuf()).cleaner();
//                if (cleaner != null) cleaner.clean();
//                cleaner = ((DirectBuffer) getWbuf()).cleaner();
//                if (cleaner != null) cleaner.clean();
//            }
        } catch (Throwable t) {
            getLogger().error("Exception cleaning ByteBuffer.", t);
        }
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getCreateTime()
     */
    @Override
    public long getCreateTime() {
        return stTime;
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#setConnectTime(long)
     */
    @Override
    public void setConnectTime(long cTime) {
        this.stTime = cTime;
//        if(reconnectCounter == null) {
//            final List<Tag> tags = new ArrayList<Tag>(5);
//            tags.add(new BasicTag(EVCacheMetricsFactory.CACHE, client.getAppName()));
//            tags.add(new BasicTag(EVCacheMetricsFactory.SERVERGROUP, client.getServerGroupName()));
//            tags.add(new BasicTag(EVCacheMetricsFactory.ZONE, client.getZone()));
//            tags.add(new BasicTag(EVCacheMetricsFactory.HOST, hostName));
//            tags.add(new BasicTag(EVCacheMetricsFactory.CONFIG_NAME, EVCacheMetricsFactory.RECONNECT));
//            this.reconnectCounter = EVCacheMetricsFactory.getInstance().getCounter(EVCacheMetricsFactory.INTERNAL_RECONNECT, tags);
//            
//        }
//        reconnectCounter.increment();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getAppName()
     */
    @Override
    public String getAppName() {
        return client.getAppName();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getHostName()
     */
    @Override
    public String getHostName() {
        return hostName;
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getServerGroup()
     */
    @Override
    public ServerGroup getServerGroup() {
        return client.getServerGroup();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getId()
     */
    @Override
    public int getId() {
        return client.getId();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getTags()
     */
    @Override
    public List<Tag> getTags() {
        return client.getTagList();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getTotalReconnectCount()
     */
    @Override
    public int getTotalReconnectCount() {
//        if(reconnectCounter == null) return 0;
//        return (int)reconnectCounter.count();
        return getReconnectCount();
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getSocketChannelLocalAddress()
     */
    @Override
    public String getSocketChannelLocalAddress() {
        try {
            if(getChannel() != null) {
                return getChannel().getLocalAddress().toString();
            }
        } catch (IOException e) {
            log.error("Exception", e);
        }
        return "NULL";
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getSocketChannelRemoteAddress()
     */
    @Override
    public String getSocketChannelRemoteAddress() {
        try {
            if(getChannel() != null) {
                return getChannel().getRemoteAddress().toString();
            }
        } catch (IOException e) {
            log.error("Exception", e);
        }
        return "NULL";
    }

    /* (non-Javadoc)
     * @see net.spy.memcached.protocol.binary.EVCacheNode1#getConnectTime()
     */
    @Override
    public String getConnectTime() {
        return ISODateTimeFormat.dateTime().print(stTime);
    }
}
