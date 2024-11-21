package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.Selector;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.spy.memcached.ops.Operation;

public class EVCacheConnection extends MemcachedConnection {
    private static final Logger log = LoggerFactory.getLogger(EVCacheConnection.class);
    private final net.spy.memcached.compat.log.Logger spyLogger;

    public EVCacheConnection(String name, int bufSize, ConnectionFactory f,
            List<InetSocketAddress> a, Collection<ConnectionObserver> obs,
            FailureMode fm, OperationFactory opfactory) throws IOException {
        super(bufSize, f, a, obs, fm, opfactory);
        setName(name);
        spyLogger = super.getLogger();
    }

    @Override
    public void shutdown() throws IOException {
        try {
            super.shutdown();
            for (MemcachedNode qa : getLocator().getAll()) {
                if (qa instanceof EVCacheNode) {
                    ((EVCacheNode) qa).shutdown();
                }
            }
        } finally {
            if(running) {
                running = false;
                if(log.isWarnEnabled()) log.warn("Forceful shutdown by interrupting the thread.", new Exception());
                interrupt();
            }
        }
    }

    public void run() {
        while (running) {
            try {
                handleIO();
            } catch (IOException e) {
                if (log.isDebugEnabled()) log.debug(e.getMessage(), e);
            } catch (CancelledKeyException e) {
                if (log.isDebugEnabled()) log.debug(e.getMessage(), e);
            } catch (ClosedSelectorException e) {
                if (log.isDebugEnabled()) log.debug(e.getMessage(), e);
            } catch (IllegalStateException e) {
                if (log.isDebugEnabled()) log.debug(e.getMessage(), e);
            } catch (ConcurrentModificationException e) {
                if (log.isDebugEnabled()) log.debug(e.getMessage(), e);
            } catch (Throwable e) {
                log.error("SEVERE EVCACHE ISSUE.", e);// This ensures the thread
                                                      // doesn't die
            }
        }
        if (log.isDebugEnabled()) log.debug(toString() + " : Shutdown");
    }

    public String toString() {
        return super.toString();
    }

    protected void addOperation(final MemcachedNode node, final Operation o) {
        super.addOperation(node, o);
        ((EVCacheNode) node).incrOps();
    }

    @Override
    public void addOperations(Map<MemcachedNode, Operation> ops) {
        // Directly inline this operation, copied down from parent implementation.
        final String OVERALL_REQUEST_METRIC = "[MEM] Request Rate: All";

        // We want to avoid selector.wakeup() being called repeatedly if we can avoid it.
        // The epoll selector takes a lock underneath even if it determines that no system
        // call is required. This is unfortunately contending with the memcached loop which
        // is prevented from progressing the select() while a wakeup() check is in flight.

        boolean addedOne = false;
        try {
            for (Map.Entry<MemcachedNode, Operation> me : ops.entrySet()) {
                MemcachedNode node = me.getKey();
                Operation o = me.getValue();

                if (!node.isAuthenticated()) {
                    retryOperation(o);
                    continue;
                }

                addedOne = true;

                o.setHandlingNode(node);
                o.initialize();
                node.addOp(o);
                addedQueue.offer(node);

                ((EVCacheNode) node).incrOps();
                metrics.markMeter(OVERALL_REQUEST_METRIC);
                // retain the existing logger configuration
                spyLogger.debug("Added %s to %s", o, node);
            }
        } finally {
            if (addedOne) {
                Selector s = selector.wakeup();
                assert s == selector : "Wakeup returned the wrong selector.";
            }
        }
    }

    @Override
    public void enqueueOperation(final String key, final Operation o) {
        checkState();
        addOperation(key, o);
      }
    

    @Override
    public CountDownLatch broadcastOperation(BroadcastOpFactory of, Collection<MemcachedNode> nodes) {
        for (MemcachedNode node : nodes) {
            ((EVCacheNode) node).incrOps();
        }
        return super.broadcastOperation(of, nodes);
    }
    
}
