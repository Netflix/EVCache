package com.netflix.evcache.pool.eureka.connection;

import java.util.List;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.KetamaNodeLocator;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;

/**
 * ConnectionFactory for eureka based implementation.
 *
 * <p>
 * This implementation creates connections where the operation queue is an
 * ArrayBlockingQueue and the read and write queues are unbounded
 * LinkedBlockingQueues.  The <code>Redistribute</code> FailureMode is always
 * used.  If other FailureModes are needed, look at the
 * ConnectionFactoryBuilder.
 *
 * </p>
 */
public class EVCacheConnectionFactory extends BinaryConnectionFactory {

    private final String appName;

    /**
     * Creates an instance of {@link ConnectionFactory} for the given appName, queue length and Ketama Hashing.
     *
     * @param appName - the name of the EVCache app
     * @param len the length of the operation queue
     */
    public EVCacheConnectionFactory(String appName, int len) {
        super(len, BinaryConnectionFactory.DEFAULT_READ_BUFFER_SIZE, HashAlgorithm.KETAMA_HASH);
        this.appName = appName;
    }

    /**
     * returns a instance of {@link KetamaNodeLocator}.
     */
    public NodeLocator createLocator(List<MemcachedNode> list) {
        return new KetamaNodeLocator(list, HashAlgorithm.KETAMA_HASH, new EVCacheKetamaNodeLocatorConfiguration(appName));
    }
}
