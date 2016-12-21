package com.netflix.evcache.test.proxy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.netflix.evcache.EVCache;
import com.netflix.evcache.pool.EVCacheClientPool;

import net.spy.memcached.CachedData;

public class RelayHandler {

	public static final byte REQ_MAGIC = (byte) 0x80;
	public static final byte RES_MAGIC = (byte) 0x81;
	public static final byte GET_CMD = 0x00;
	
	private final SocketChannel clientChannel;
	private final EVCacheClientPool pool;
	public RelayHandler(SocketChannel clientChannel, EVCacheClientPool pool) {
		this.clientChannel = clientChannel;
		this.pool = pool;
		handle();
		System.out.println("New client - " + clientChannel);
	}
	
	public void handle() {
		if ((clientChannel != null) && (clientChannel.isOpen())) {
			while (true) {
				try {
					ByteBuffer buffer = ByteBuffer.allocate(255);
					int result = clientChannel.read(buffer);
					System.out.println("\n\nBytes Read from client - " + result);
					if(result == -1) break;

					if(result == 0) continue;
					buffer.flip();
					byte magic = buffer.get();
					if(magic != REQ_MAGIC) {
						System.out.println("Invalid magic - " + magic);
						continue;
					}
					
					byte cmd = buffer.get();
					System.out.println("cmd - " + cmd);
					short keyLen = buffer.getShort();
				    byte extraLen = buffer.get();
				    byte notUsed = buffer.get();
				    short vbucket = buffer.getShort();
				    int totalLen = buffer.getInt();
				    int opaque = buffer.getInt();
				    long cas = buffer.getLong();
				    if(extraLen > 0) {
					    byte[] extraBytes = new byte[extraLen];
					    buffer.get(extraBytes);
				    }
					String message = null;
				    if(keyLen > 0) {
					    byte[] keyBytes = new byte[keyLen];
					    buffer.get(keyBytes);
					    message = new String(keyBytes);
				    }
				    buffer.rewind();
				    if(message == null && cmd == (byte)0x0b) {
				    	message = "ping";
				    }
				    SocketChannel channel = pool.getEVCacheClientForRead().getEVCacheMemcachedClient().getEVCacheNode(message).getChannel();
				    while(!channel.isConnected()) {
				    	System.out.println("Waiting as the channel is yet to be connected.");
				    	Thread.sleep(100);
				    }
				    int remaining = 0;
				    while((remaining = buffer.remaining()) > 0) {
				    	int wrote = channel.write(buffer);
				    	System.out.println("Remaining : " + remaining + ", Wrote to memcached = " + wrote);
				    }
				    ByteBuffer buf = ByteBuffer.allocate(128);
				    int numRead = 0, totalRead = 0;
				    while((numRead = channel.read(buf)) >= 0) {
				    	if(numRead > 0) {
					    	totalRead += numRead;
					    	System.out.println("Read from memcached = " + numRead);
					    	buf.flip();
					    	int wrote = clientChannel.write(buf);
					    	System.out.println("Wrote to client = " + wrote);
					    	buf.clear();
				    	} else {
				    		if(totalRead == 0) {
				    			Thread.sleep(1);
				    		} else {
				    			break;
				    		}
				    	}
				    }
				    System.out.println("Total read from memcached and sent to client = " + totalRead);
				    /*
				    if(cmd == 0x11) {
						byte[] nullBytes = "NA".getBytes();
						buf.put(RES_MAGIC);
						buf.put(cmd);
						buf.putShort((short)0);
						buf.put((byte)0x00);
						buf.put((byte)0x00);
						buf.putShort((short)0);
						buf.putInt(nullBytes.length);
						buf.putInt(opaque);
						buf.putLong(cas);
						buf.put(nullBytes);
						buf.flip();
						int numOfBytesWritten = clientChannel.write(buf);
						System.out.println("Number of bytes sent " + numOfBytesWritten);
						buf.clear();

				    	clientChannel.write(buf);
				    }
				    */
				    
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			try {
				clientChannel.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

