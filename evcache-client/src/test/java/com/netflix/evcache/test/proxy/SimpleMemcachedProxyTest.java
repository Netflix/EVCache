package com.netflix.evcache.test.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.net.SocketOptions;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutionException;

import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheException;
import com.netflix.evcache.pool.EVCacheClientPoolManager;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

public class SimpleMemcachedProxyTest {

	public static void main (String [] args) throws Exception {
		Thread t1 = new Thread( new Runnable() {
			public void run() {
				try {
					new SimpleMemcachedProxyTest().go(11211, "localhost");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		t1.start();

	}

	private void go(int port, String server) throws IOException, InterruptedException, ExecutionException, EVCacheException {

		System.setProperty("EVCACHE.use.simple.node.list.provider", "true");
		System.setProperty("EVCACHE.EVCacheClientPool.readTimeout", "1000");
		System.setProperty("EVCACHE.operation.timeout", "100000");
		System.setProperty("EVCACHE.EVCacheClientPool.bulkReadTimeout", "10000");
		System.setProperty("EVCACHE-NODES","evcache-" + port+"="+server+":11200");

		EVCacheClientPoolManager.getInstance().initEVCache("EVCACHE");
		//EVCache evCache = (new EVCache.Builder()).setAppName("EVCACHE").setCachePrefix(null).enableRetry().setTranscoder(new ChunkTranscoder()).build();

		ServerSocketChannel serverChannel = ServerSocketChannel.open();
		InetSocketAddress hostAddress = new InetSocketAddress("localhost", port);
		serverChannel.bind(hostAddress);

		System.out.println("Server channel bound to port: " + hostAddress.getPort());
		System.out.println("Waiting for client to connect... ");
		while(true) {
			SocketChannel clientChannel = serverChannel.accept();
			Thread t1 = new Thread( new Runnable() {
				public void run() {
					try {
						RelayHandler proxyHandler = new RelayHandler(clientChannel, EVCacheClientPoolManager.getInstance().getEVCacheClientPool("EVCACHE"));
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
			t1.start();
		}
	}

	/**
	 * A local transcoder used only by EVCache client to ensure we don't try to deserialize chunks
	 * 
	 * @author smadappa
	 *
	 */
	class ChunkTranscoder implements Transcoder<CachedData> {

		public ChunkTranscoder() {
		}

		public boolean asyncDecode(CachedData d) {
			return false;
		}

		public CachedData decode(CachedData d) {
			return d;
		}

		public CachedData encode(CachedData o) {
			return o;
		}

		public int getMaxSize() {
			return CachedData.MAX_SIZE;
		}

	}

}


