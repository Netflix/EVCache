package net.spy.memcached.protocol.ascii;


import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

public class MetaGetOperationImpl extends EVCacheOperationImpl implements MetaGetOperation {
    private static final Logger log = LoggerFactory.getLogger(MetaGetOperationImpl.class);

    private static final OperationStatus END = new OperationStatus(true, "EN", StatusCode.SUCCESS);
    private static final int OVERHEAD = 32;
    private final MetaGetOperation.Callback cb;

    private final String key;
    private int currentFlag = -1;
    private byte[] data = null;
    private int readOffset = 0;
    private byte lookingFor = '\0';

    public MetaGetOperationImpl(String k, MetaGetOperation.Callback cb) {
        super(cb);
        this.key = k;
        this.cb = cb;
    }

    @Override
    public void handleLine(String line) {
        if(log.isDebugEnabled()) log.debug("meta get of {} returned {}", key, line);
        if (line.length() == 0 || line.equals("EN")) {
            getCallback().receivedStatus(END);
            transitionState(OperationState.COMPLETE);
        } else if (line.startsWith("VA")) {
            String[] parts = line.split(" ");
            if(log.isDebugEnabled()) log.debug("Num of parts "+ parts.length);
            if(parts.length <= 2) return;

            int size = Integer.parseInt(parts[1]);
            if(log.isDebugEnabled()) log.debug("Size of value in bytes : "+ size);
            data = new byte[size];

            for(int i = 2; i < parts.length; i++) {
                final char flag = parts[i].charAt(0);
                final String val = parts[i].substring(1);
                if(log.isDebugEnabled()) log.debug("flag="+ flag + "; Val=" + val);
                cb.gotMetaData(key, flag, val);
                if(flag == 'f') currentFlag = Integer.parseInt(val);
            }
            setReadType(OperationReadType.DATA);
        }
    }

    public void handleRead(ByteBuffer b) {
        if(log.isDebugEnabled()) log.debug("readOffset: {}, length: {}", readOffset, data.length);
        // If we're not looking for termination, we're still looking for data
        if (lookingFor == '\0') {
            int toRead = data.length - readOffset;
            int available = b.remaining();
            toRead = Math.min(toRead, available);
            if(log.isDebugEnabled()) log.debug("Reading {} bytes", toRead);
            b.get(data, readOffset, toRead);
            readOffset += toRead;
        }
        // Transition us into a ``looking for \r\n'' kind of state if we've
        // read enough and are still in a data state.
        if (readOffset == data.length && lookingFor == '\0') {
            // The callback is most likely a get callback. If it's not, then
            // it's a gets callback.
            OperationCallback cb = getCallback();
            if (cb instanceof MetaGetOperation.Callback) {
                MetaGetOperation.Callback mgcb = (MetaGetOperation.Callback) cb;
                mgcb.gotData(key, currentFlag, data);
            }
            lookingFor = '\r';
        }
        // If we're looking for an ending byte, let's go find it.
        if (lookingFor != '\0' && b.hasRemaining()) {
            do {
                byte tmp = b.get();
                assert tmp == lookingFor : "Expecting " + lookingFor + ", got "
                        + (char) tmp;
                switch (lookingFor) {
                case '\r':
                    lookingFor = '\n';
                    
                    break;
                case '\n':
                    lookingFor = '\0';
                    break;
                default:
                    assert false : "Looking for unexpected char: " + (char) lookingFor;
                }
            } while (lookingFor != '\0' && b.hasRemaining());
            // Completed the read, reset stuff.
            if (lookingFor == '\0') {
                data = null;
                readOffset = 0;
                currentFlag = -1;
                getCallback().receivedStatus(END);
                transitionState(OperationState.COMPLETE);
                getLogger().debug("Setting read type back to line.");
                setReadType(OperationReadType.LINE);
            }
        }
    }


    @Override
    public void initialize() {
    	final String flags = "s f t h l c v";
    	final ByteBuffer b = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length + flags.length() + OVERHEAD);
        setArguments(b, "mg", key, flags);
        b.flip();
        setBuffer(b);
    }

    @Override
    public String toString() {
        return "Cmd: me Key: " + key;
    }

}
