package net.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

/**
 * Implementation of MetaDeleteOperation using memcached meta protocol.
 * Supports advanced delete features like CAS-based conditional deletes and invalidation.
 */
public class MetaDeleteOperationImpl extends EVCacheOperationImpl implements MetaDeleteOperation {
    private static final Logger log = LoggerFactory.getLogger(MetaDeleteOperationImpl.class);

    private static final OperationStatus DELETED = new OperationStatus(true, "HD", StatusCode.SUCCESS);
    private static final OperationStatus NOT_FOUND = new OperationStatus(false, "NF", StatusCode.SUCCESS);
    private static final OperationStatus EXISTS = new OperationStatus(false, "EX", StatusCode.SUCCESS);

    private final MetaDeleteOperation.Callback cb;
    private final Builder builder;
    
    private boolean deleted = false;
    private long returnedCas = 0;

    public MetaDeleteOperationImpl(Builder builder, MetaDeleteOperation.Callback cb) {
        super(cb);
        this.builder = builder;
        this.cb = cb;
    }

    @Override
    public void handleLine(String line) {
        if (log.isDebugEnabled()) {
            log.debug("meta delete of {} returned {}", builder.getKey(), line);
        }
        
        if (line.equals("HD")) {
            deleted = true;
            cb.deleteComplete(builder.getKey(), true);
            getCallback().receivedStatus(DELETED);
            transitionState(OperationState.COMPLETE);
        } else if (line.equals("NF")) {
            cb.deleteComplete(builder.getKey(), false);
            getCallback().receivedStatus(NOT_FOUND);
            transitionState(OperationState.COMPLETE);
        } else if (line.equals("EX")) {
            // CAS mismatch - item exists but CAS doesn't match
            cb.deleteComplete(builder.getKey(), false);
            getCallback().receivedStatus(EXISTS);
            transitionState(OperationState.COMPLETE);
        } else if (line.startsWith("HD ") || line.startsWith("NF ") || line.startsWith("EX ")) {
            // Parse metadata returned with response
            String[] parts = line.split(" ");
            deleted = parts[0].equals("HD");
            
            // Parse returned metadata flags
            for (int i = 1; i < parts.length; i++) {
                if (parts[i].length() > 0) {
                    char flag = parts[i].charAt(0);
                    String value = parts[i].substring(1);
                    
                    if (flag == 'c') {
                        returnedCas = Long.parseLong(value);
                    }
                    
                    cb.gotMetaData(builder.getKey(), flag, value);
                }
            }
            
            cb.deleteComplete(builder.getKey(), deleted);
            getCallback().receivedStatus(deleted ? DELETED : NOT_FOUND);
            transitionState(OperationState.COMPLETE);
        }
    }

    @Override
    public void initialize() {
        // Meta delete command syntax: md <key> <flags>*\r\n
        List<String> flags = new ArrayList<>();
        
        // Add delete mode flag (I=invalidate instead of delete)
        if (builder.getMode() == DeleteMode.INVALIDATE) {
            flags.add("I");
        }
        
        // Add CAS if specified (C<token>)
        if (builder.getCas() > 0) {
            flags.add("C" + builder.getCas());
        }
        
        // Request metadata returns
        if (builder.isReturnCas()) {
            flags.add("c"); // Return CAS token
        }
        
        if (builder.isReturnTtl()) {
            flags.add("t"); // Return TTL
        }
        
        if (builder.isReturnSize()) {
            flags.add("s"); // Return size
        }
        
        // Quiet mode (no response for success)
        if (builder.isQuiet()) {
            flags.add("q");
        }
        
        // Calculate buffer size
        byte[] keyBytes = KeyUtil.getKeyBytes(builder.getKey());
        StringBuilder cmdBuilder = new StringBuilder();
        cmdBuilder.append("md ").append(builder.getKey());
        
        // Add flags
        for (String flag : flags) {
            cmdBuilder.append(" ").append(flag);
        }
        cmdBuilder.append("\r\n");
        
        byte[] cmdBytes = cmdBuilder.toString().getBytes();
        ByteBuffer b = ByteBuffer.allocate(cmdBytes.length);
        b.put(cmdBytes);
        
        b.flip();
        setBuffer(b);
    }

    @Override
    public String toString() {
        return "Cmd: md Key: " + builder.getKey() + " Mode: " + builder.getMode();
    }
}