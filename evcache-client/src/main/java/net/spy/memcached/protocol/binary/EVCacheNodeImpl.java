package net.spy.memcached.protocol.binary;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.CompositeMonitor;
import com.netflix.servo.monitor.LongGauge;
import com.netflix.servo.monitor.Monitor;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.tag.BasicTagList;
import com.netflix.servo.tag.TagList;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ops.Operation;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({ "FCBL_FIELD_COULD_BE_LOCAL", "EXS_EXCEPTION_SOFTENING_NO_CHECKED",
        "REC_CATCH_EXCEPTION",
        "SCII_SPOILED_CHILD_INTERFACE_IMPLEMENTATOR" })
public class EVCacheNodeImpl extends BinaryMemcachedNodeImpl implements EVCacheNodeImplMBean, CompositeMonitor<Long> {
    private static final Logger log = LoggerFactory.getLogger(EVCacheNodeImpl.class);

    protected long stTime;
    protected final AtomicLong opCount = new AtomicLong(0);
    protected final AtomicInteger reconnectCount = new AtomicInteger(0);

    protected final String _appName;
    protected final String hostName;
    protected final ServerGroup _serverGroup;
    protected final int id;
    protected final BlockingQueue<Operation> readQ;
    protected final BlockingQueue<Operation> inputQueue;
    protected final String metricPrefix;
    protected final DynamicBooleanProperty sendMetrics;
    protected final MonitorConfig baseConfig;
    protected final TagList baseTags;
    protected final TagList tags;

    private long timeoutStartTime;

    public EVCacheNodeImpl(SocketAddress sa, SocketChannel c, int bufSize, BlockingQueue<Operation> rq,
            BlockingQueue<Operation> wq, BlockingQueue<Operation> iq,
            long opQueueMaxBlockTimeMillis, boolean waitForAuth, long dt, long at, ConnectionFactory fa, String appName,
            int id, ServerGroup serverGroup, long stTime) {
        super(sa, c, bufSize, rq, wq, iq, Long.valueOf(opQueueMaxBlockTimeMillis), waitForAuth, dt, at, fa);

        this.id = id;
        this._appName = appName;
        this._serverGroup = serverGroup;
        setConnectTime(stTime);
        this.readQ = rq;
        this.inputQueue = iq;
        this.sendMetrics = EVCacheConfig.getInstance().getDynamicBooleanProperty("EVCacheNodeImpl." + appName + ".sendMetrics", false);
        this.tags = BasicTagList.of("ServerGroup", _serverGroup.getName(), "APP", appName, "Id", String.valueOf(id), EVCacheMetricsFactory.OWNER.getKey(), EVCacheMetricsFactory.OWNER.getValue());
        this.hostName = ((InetSocketAddress) getSocketAddress()).getHostName();
        this.metricPrefix = "EVCacheNode";
        this.baseConfig = MonitorConfig.builder(metricPrefix).build();
        baseTags = BasicTagList.concat(tags, BasicTagList.of("HOST", hostName));
        setupMonitoring();
    }

    private String getMonitorName() {
        return "com.netflix.evcache:Group=" + _appName + ",SubGroup=pool" + ",SubSubGroup=" + _serverGroup.getName()
                + ",SubSubSubGroup=" + id + ",SubSubSubSubGroup=" + hostName
                + "_" + stTime;
    }

    private void setupMonitoring() {
        try {
            final ObjectName mBeanName = ObjectName.getInstance(getMonitorName());
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mbeanServer.isRegistered(mBeanName)) {
                if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanName
                        + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanName);
            }
            mbeanServer.registerMBean(this, mBeanName);
        } catch (Exception e) {
            if (log.isWarnEnabled()) log.warn("Exception while setting up monitoring.", e);
        }
    }

    public void registerMonitors() {
        try {
            Monitors.registerObject(this);
        } catch (Exception e) {
            if (log.isWarnEnabled()) log.warn("Exception while registering.", e);
        }
    }

    public boolean isAvailable() {
        return isActive();
    }

    public int getWriteQueueSize() {
        return writeQ.size();
    }

    public int getReadQueueSize() {
        return readQ.size();
    }

    public int getInputQueueSize() {
        return inputQueue.size();
    }

    public long incrOps() {
        return opCount.incrementAndGet();
    }

    public long getNumOfOps() {
        return opCount.get();
    }

    public void flushInputQueue() {
        inputQueue.clear();
    }

    public long getStartTime() {
        return stTime;
    }

    public long getTimeoutStartTime() {
        return timeoutStartTime;
    }

    public void removeMonitoring() {
        try {
            final ObjectName mBeanName = ObjectName.getInstance(getMonitorName());
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mbeanServer.isRegistered(mBeanName)) {
                if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanName
                        + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanName);
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug("Exception while setting up the monitoring.", e);
        }
    }

    public void shutdown() {
        removeMonitoring();
        writeQ.clear();
        readQ.clear();
        inputQueue.clear();
        try {
            // Cleanup the ByteBuffers only if they are sun.nio.ch.DirectBuffer
            // If we don't cleanup then we will leak 16K of memory
            if (getRbuf() instanceof DirectBuffer) {
                Cleaner cleaner = ((DirectBuffer) getRbuf()).cleaner();
                if (cleaner != null) cleaner.clean();
                cleaner = ((DirectBuffer) getWbuf()).cleaner();
                if (cleaner != null) cleaner.clean();
            }
        } catch (Throwable t) {
            getLogger().error("Exception cleaning ByteBuffer.", t);
        }
    }

    @Override
    public Long getValue() {
        return getValue(0);
    }

    @Override
    public Long getValue(int pollerIndex) {
        return (sendMetrics.get()) ? Long.valueOf(3) : Long.valueOf(0);
    }

    @Override
    public MonitorConfig getConfig() {
        return baseConfig;
    }

    @Override
    public List<Monitor<?>> getMonitors() {
        if (!sendMetrics.get() && getContinuousTimeout() == 0) return Collections.<Monitor<?>> emptyList();

        try {
            final List<Monitor<?>> metrics = new ArrayList<Monitor<?>>();
            if(getContinuousTimeout() > 0) {
                MonitorConfig monitorConfig = EVCacheConfig.getInstance().getMonitorConfig(metricPrefix + "_ContinuousTimeout", DataSourceType.GAUGE, baseTags);
                final LongGauge cTimeouts = new LongGauge(monitorConfig);
                cTimeouts.set(Long.valueOf(getContinuousTimeout()));
                metrics.add(cTimeouts);
            }

            if (sendMetrics.get()) {
                MonitorConfig monitorConfig = EVCacheConfig.getInstance().getMonitorConfig(metricPrefix + "_WriteQ",
                        DataSourceType.GAUGE, baseTags);
                final LongGauge wQueue = new LongGauge(monitorConfig);
                wQueue.set(Long.valueOf(writeQ.size()));
                metrics.add(wQueue);

                monitorConfig = EVCacheConfig.getInstance().getMonitorConfig(metricPrefix + "_ReadQ", DataSourceType.GAUGE,
                        baseTags);
                final LongGauge rQueue = new LongGauge(monitorConfig);
                rQueue.set(Long.valueOf(readQ.size()));
                metrics.add(rQueue);

                monitorConfig = EVCacheConfig.getInstance().getMonitorConfig(metricPrefix + "_NumOfOps",
                        DataSourceType.COUNTER, baseTags);
                final BasicCounter counter = new BasicCounter(monitorConfig);
                counter.increment(opCount.get());
                metrics.add(counter);
            }
            return metrics;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return Collections.<Monitor<?>> emptyList();
    }

    public long getCreateTime() {
        return stTime;
    }

    public void setConnectTime(long cTime) {
        this.stTime = cTime;
        reconnectCount.incrementAndGet();
    }

    public String getAppName() {
        return _appName;
    }

    public String getHostName() {
        return hostName;
    }

    public ServerGroup getServerGroup() {
        return _serverGroup;
    }

    public int getId() {
        return id;
    }

    public TagList getBaseTags() {
		return baseTags;
	}
    
    public int getTotalReconnectCount() {
        return reconnectCount.get();
    }

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

	@Override
	public String getConnectTime() {
		return ISODateTimeFormat.dateTime().print(stTime);
	}
    
    
}
