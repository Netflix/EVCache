package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.binary.EVCacheNodeImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EVCacheConnection extends MemcachedConnection {
    private static Logger log = LoggerFactory.getLogger(EVCacheConnection.class);

    public EVCacheConnection(String name, int bufSize, ConnectionFactory f,
            List<InetSocketAddress> a, Collection<ConnectionObserver> obs,
            FailureMode fm, OperationFactory opfactory) throws IOException {
        super(bufSize, f, a, obs, fm, opfactory);
        setName(name);
    }

    @Override
    public void shutdown() throws IOException {
        super.shutdown();
        for (MemcachedNode qa : getLocator().getAll()) {
            if (qa instanceof EVCacheNodeImpl) {
                ((EVCacheNodeImpl) qa).shutdown();
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
        ((EVCacheNodeImpl) node).incrOps();
    }

    @Override
    public void addOperations(Map<MemcachedNode, Operation> ops) {
        super.addOperations(ops);
        for (MemcachedNode node : ops.keySet()) {
            ((EVCacheNodeImpl) node).incrOps();
        }
    }

    @Override
    public CountDownLatch broadcastOperation(BroadcastOpFactory of, Collection<MemcachedNode> nodes) {
        for (MemcachedNode node : nodes) {
            ((EVCacheNodeImpl) node).incrOps();
        }
        return super.broadcastOperation(of, nodes);
    }
}
