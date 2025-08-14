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
 * Implementation of MetaSetOperation using memcached meta protocol.
 * Supports advanced set features like CAS, conditional sets, and metadata retrieval.
 */
public class MetaSetOperationImpl extends EVCacheOperationImpl implements MetaSetOperation {
    private static final Logger log = LoggerFactory.getLogger(MetaSetOperationImpl.class);

    private static final OperationStatus STORED = new OperationStatus(true, "HD", StatusCode.SUCCESS);
    private static final OperationStatus NOT_STORED = new OperationStatus(false, "NS", StatusCode.SUCCESS);
    private static final OperationStatus EXISTS = new OperationStatus(false, "EX", StatusCode.SUCCESS);
    private static final OperationStatus NOT_FOUND = new OperationStatus(false, "NF", StatusCode.SUCCESS);

    private final MetaSetOperation.Callback cb;
    private final Builder builder;
    
    private boolean stored = false;
    private long returnedCas = 0;

    public MetaSetOperationImpl(Builder builder, MetaSetOperation.Callback cb) {
        super(cb);
        this.builder = builder;
        this.cb = cb;
    }

    @Override
    public void handleLine(String line) {
        if (log.isDebugEnabled()) {
            log.debug("meta set of {} returned {}", builder.getKey(), line);
        }
        
        if (line.equals("HD")) {
            stored = true;
            cb.setComplete(builder.getKey(), returnedCas, true);
            getCallback().receivedStatus(STORED);
            transitionState(OperationState.COMPLETE);
        } else if (line.equals("NS")) {
            cb.setComplete(builder.getKey(), returnedCas, false);
            getCallback().receivedStatus(NOT_STORED);
            transitionState(OperationState.COMPLETE);
        } else if (line.equals("EX")) {
            cb.setComplete(builder.getKey(), returnedCas, false);
            getCallback().receivedStatus(EXISTS);
            transitionState(OperationState.COMPLETE);
        } else if (line.equals("NF")) {
            cb.setComplete(builder.getKey(), returnedCas, false);
            getCallback().receivedStatus(NOT_FOUND);
            transitionState(OperationState.COMPLETE);
        } else if (line.startsWith("HD ") || line.startsWith("NS ") || line.startsWith("EX ") || line.startsWith("NF ")) {
            // Parse metadata returned with response
            String[] parts = line.split(" ");
            stored = parts[0].equals("HD");
            
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
            
            cb.setComplete(builder.getKey(), returnedCas, stored);
            getCallback().receivedStatus(stored ? STORED : NOT_STORED);
            transitionState(OperationState.COMPLETE);
        }
    }

    @Override
    public void initialize() {
        // Meta set command syntax: ms <key> <datalen> <flags>*\r\n<data>\r\n
        List<String> flags = new ArrayList<>();
        
        // Add mode flag (S=set, N=add, R=replace, A=append, P=prepend)
        flags.add(builder.getMode().getFlag());
        
        // Add CAS if specified (C<token>)
        if (builder.getCas() > 0) {
            flags.add("C" + builder.getCas());
        }
        
        // Add client flags if non-zero (F<flags>)  
        if (builder.getFlags() != 0) {
            flags.add("F" + builder.getFlags());
        }
        
        // Add TTL if specified (T<ttl>)
        if (builder.getExpiration() > 0) {
            flags.add("T" + builder.getExpiration());
        }
        
        // Request metadata returns
        if (builder.isReturnCas()) {
            flags.add("c"); // Return CAS token
        }
        
        if (builder.isReturnTtl()) {
            flags.add("t"); // Return TTL
        }
        
        // Mark as stale if requested (I - invalidate/mark stale)
        if (builder.isMarkStale()) {
            flags.add("I");
        }
        
        // Calculate buffer size
        byte[] keyBytes = KeyUtil.getKeyBytes(builder.getKey());
        byte[] valueBytes = builder.getValue();
        StringBuilder cmdBuilder = new StringBuilder();
        cmdBuilder.append("ms ").append(builder.getKey()).append(" ").append(valueBytes.length);
        
        // Add flags
        for (String flag : flags) {
            cmdBuilder.append(" ").append(flag);
        }
        cmdBuilder.append("\r\n");
        
        byte[] cmdBytes = cmdBuilder.toString().getBytes();
        int totalSize = cmdBytes.length + valueBytes.length + 2; // +2 for final \r\n
        
        ByteBuffer b = ByteBuffer.allocate(totalSize);
        b.put(cmdBytes);
        b.put(valueBytes);
        b.put((byte) '\r');
        b.put((byte) '\n');
        
        b.flip();
        setBuffer(b);
    }

    @Override
    public String toString() {
        return "Cmd: ms Key: " + builder.getKey() + " Mode: " + builder.getMode();
    }
}