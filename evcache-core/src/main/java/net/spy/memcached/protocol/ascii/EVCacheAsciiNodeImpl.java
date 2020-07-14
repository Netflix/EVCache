package net.spy.memcached.protocol.ascii;


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
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.protocol.ProxyCallback;
import net.spy.memcached.protocol.TCPMemcachedNodeImpl;
public class EVCacheAsciiNodeImpl extends TCPMemcachedNodeImpl implements EVCacheNodeMBean, EVCacheNode {

    private static final Logger log = LoggerFactory.getLogger(EVCacheAsciiNodeImpl.class);

    protected long stTime;
    protected final String hostName;
    protected final BlockingQueue<Operation> readQ;
    protected final BlockingQueue<Operation> inputQueue;
    protected final EVCacheClient client;
    private final AtomicInteger numOps = new AtomicInteger(0);
    private long timeoutStartTime;
    protected final Counter operationsCounter;
    
  public EVCacheAsciiNodeImpl(SocketAddress sa, SocketChannel c, int bufSize, BlockingQueue<Operation> rq, BlockingQueue<Operation> wq, BlockingQueue<Operation> iq,
          long opQueueMaxBlockTimeMillis, boolean waitForAuth, long dt, long at, ConnectionFactory fa, EVCacheClient client, long stTime) {
    // ASCII never does auth
    super(sa, c, bufSize, rq, wq, iq, opQueueMaxBlockTimeMillis, false, dt, at, fa);
    this.client = client;
    final String appName = client.getAppName();
    this.readQ = rq;
    this.inputQueue = iq;
    this.hostName = ((InetSocketAddress) getSocketAddress()).getHostName();
    this.operationsCounter = client.getOperationCounter();
    setConnectTime(stTime);
    setupMonitoring(appName);
  }

  @Override
  protected void optimize() {
    // make sure there are at least two get operations in a row before
    // attempting to optimize them.
    if (writeQ.peek() instanceof GetOperation) {
      optimizedOp = writeQ.remove();
      if (writeQ.peek() instanceof GetOperation) {
        OptimizedGetImpl og = new OptimizedGetImpl((GetOperation) optimizedOp);
        optimizedOp = og;

        while (writeQ.peek() instanceof GetOperation) {
          GetOperationImpl o = (GetOperationImpl) writeQ.remove();
          if (!o.isCancelled()) {
            og.addOperation(o);
          }
        }

        // Initialize the new mega get
        optimizedOp.initialize();
        assert optimizedOp.getState() == OperationState.WRITE_QUEUED;
        ProxyCallback pcb = (ProxyCallback) og.getCallback();
        getLogger().debug("Set up %s with %s keys and %s callbacks", this,
            pcb.numKeys(), pcb.numCallbacks());
      }
    }
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

  public void registerMonitors() {
  }


  public boolean isAvailable(EVCache.Call call) {
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
      operationsCounter.increment();
      return numOps.incrementAndGet();
  }

  public long getNumOfOps() {
      return numOps.get();
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

  public void shutdown() {
      removeMonitoring();
      writeQ.clear();
      readQ.clear();
      inputQueue.clear();
  }

  public long getCreateTime() {
      return stTime;
  }

  public void setConnectTime(long cTime) {
      this.stTime = cTime;
  }

  public String getAppName() {
      return client.getAppName();
  }

  public String getHostName() {
      return hostName;
  }

  public ServerGroup getServerGroup() {
      return client.getServerGroup();
  }

  public int getId() {
      return client.getId();
  }

  public List<Tag> getTags() {
      return client.getTagList();
  }

  public int getTotalReconnectCount() {
      return getReconnectCount();
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
  
  
	@Override
	public EVCacheClient getEVCacheClient() {
		return client;
	}
}
