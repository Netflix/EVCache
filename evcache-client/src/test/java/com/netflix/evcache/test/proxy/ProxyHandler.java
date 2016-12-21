package com.netflix.evcache.test.proxy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.netflix.evcache.EVCache;

import net.spy.memcached.CachedData;

public class ProxyHandler {

	public static final byte REQ_MAGIC = (byte) 0x80;
	public static final byte RES_MAGIC = (byte) 0x81;
	public static final byte GET_CMD = 0x00;
	
	private final SocketChannel clientChannel;
	private final EVCache evCache;
	public ProxyHandler(SocketChannel clientChannel, EVCache evcache) {
		this.clientChannel = clientChannel;
		this.evCache = evcache;
		handle();
	}
	
	public void handle() {
		if ((clientChannel != null) && (clientChannel.isOpen())) {
			while (true) {
				try {
					ByteBuffer buffer = ByteBuffer.allocate(255);
					int result = clientChannel.read(buffer);
					System.out.println("result - " + result);
					if(result == -1) break;

					if(result == 0) continue;
					buffer.flip();
					byte magic = buffer.get();
					if(magic != REQ_MAGIC) {
						System.out.println("Invalid magic - " + magic);
						continue;
					}
					
					byte cmd = buffer.get();
					short keyLen = buffer.getShort();
				    byte extraLen = buffer.get();
				    byte notUsed = buffer.get();
				    short vbucket = buffer.getShort();
				    int totalLen = buffer.getInt();
				    int opaque = buffer.getInt();
				    long cas = buffer.getLong();
				    byte[] keyBytes = new byte[keyLen];
				    buffer.get(keyBytes);
					if(cmd == GET_CMD) {
					    
						String message = new String(keyBytes);
						System.out.println(message);
						CachedData obj = evCache.get(message);
						System.out.println(obj);
						int valueLength = 0;
						if(obj != null) {
							valueLength = 4 + obj.getData().length;
						}

						ByteBuffer buf = ByteBuffer.allocate(24+valueLength);
						buf.put(RES_MAGIC);
						buf.put(cmd);//Opcode
						buf.putShort((short)0);//Key length
						buf.put((byte)4);//flags length (Extra length) - flags sent in extras
						buf.put((byte)0x00);//Data type
						buf.putShort((short)0);//Status
						buf.putInt(valueLength);//Total body
						buf.putInt(opaque);
						buf.putLong(cas);
						
						if(obj != null) {
							buf.putInt(obj.getFlags());//flag
							buf.put(obj.getData());
						}
						buf.flip();
						int numOfBytesWritten = clientChannel.write(buf);
						System.out.println("Number of bytes sent " + numOfBytesWritten);
						buf.clear();
					} else if(cmd == 0x0b){
						byte[] nullBytes = "1.1.1".getBytes();
						ByteBuffer buf = ByteBuffer.allocate(24+nullBytes.length);
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
					}
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

