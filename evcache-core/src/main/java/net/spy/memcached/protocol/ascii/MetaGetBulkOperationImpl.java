package net.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.operation.EVCacheItem;
import com.netflix.evcache.operation.EVCacheItemMetaData;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

/**
 * Implementation of MetaGetBulkOperation using memcached meta protocol.
 * Efficiently retrieves multiple keys with metadata in a single operation.
 */
public class MetaGetBulkOperationImpl extends EVCacheOperationImpl implements MetaGetBulkOperation {
    private static final Logger log = LoggerFactory.getLogger(MetaGetBulkOperationImpl.class);

    private static final OperationStatus END = new OperationStatus(true, "EN", StatusCode.SUCCESS);
    
    private final MetaGetBulkOperation.Callback cb;
    private final Config config;
    
    private String currentKey = null;
    private int currentFlags = 0;
    private long currentCas = 0;
    private byte[] currentData = null;
    private int readOffset = 0;
    private byte lookingFor = '\0';
    private EVCacheItemMetaData currentMetaData = null;
    
    private AtomicInteger totalKeys = new AtomicInteger(0);
    private AtomicInteger foundKeys = new AtomicInteger(0);
    private AtomicInteger notFoundKeys = new AtomicInteger(0);

    public MetaGetBulkOperationImpl(Config config, MetaGetBulkOperation.Callback cb) {
        super(cb);
        this.config = config;
        this.cb = cb;
        this.totalKeys.set(config.getKeys().size());
    }

    @Override
    public void handleLine(String line) {
        if (log.isDebugEnabled()) {
            log.debug("meta get bulk returned: {}", line);
        }
        
        if (line.length() == 0 || line.equals("EN")) {
            // End of bulk operation
            cb.bulkComplete(totalKeys.get(), foundKeys.get(), notFoundKeys.get());
            getCallback().receivedStatus(END);
            transitionState(OperationState.COMPLETE);
        } else if (line.startsWith("VA ")) {
            // Value with metadata: VA <size> <key> [metadata_flags...]
            parseBulkValue(line);
            setReadType(OperationReadType.DATA);
        } else if (line.startsWith("HD ")) {
            // Hit without data (metadata only): HD <key> [metadata_flags...]
            parseBulkHit(line);
        } else if (line.startsWith("NF ") || line.startsWith("MS ")) {
            // Not found or miss: NF <key> or MS <key>
            parseBulkMiss(line);
        }
    }

    private void parseBulkValue(String line) {
        String[] parts = line.split(" ");
        if (parts.length < 3) return;
        
        int size = Integer.parseInt(parts[1]);
        currentKey = parts[2];
        currentData = new byte[size];
        readOffset = 0;
        lookingFor = '\0';
        currentMetaData = new EVCacheItemMetaData();
        
        // Parse metadata flags
        parseMetadata(currentKey, parts, 3);
        foundKeys.incrementAndGet();
    }

    private void parseBulkHit(String line) {
        String[] parts = line.split(" ");
        if (parts.length < 2) return;
        
        currentKey = parts[1];
        currentMetaData = new EVCacheItemMetaData();
        parseMetadata(currentKey, parts, 2);
        
        // Create EVCacheItem with null data for metadata-only hit
        EVCacheItem<Object> item = new EVCacheItem<>();
        item.setData(null);
        item.setFlag(currentFlags);
        copyMetadata(item.getItemMetaData(), currentMetaData);
        
        cb.gotData(currentKey, item);
        foundKeys.incrementAndGet();
    }

    private void parseBulkMiss(String line) {
        String[] parts = line.split(" ");
        if (parts.length < 2) return;
        
        String key = parts[1];
        cb.keyNotFound(key);
        notFoundKeys.incrementAndGet();
    }

    private void parseMetadata(String key, String[] parts, int startIndex) {
        currentFlags = 0;
        currentCas = 0;
        
        for (int i = startIndex; i < parts.length; i++) {
            if (parts[i].length() > 0) {
                char flag = parts[i].charAt(0);
                String value = parts[i].substring(1);
                
                // Parse commonly used metadata into EVCacheItemMetaData
                switch (flag) {
                    case 'f':
                        currentFlags = Integer.parseInt(value);
                        break;
                    case 'c':
                        currentCas = Long.parseLong(value);
                        if (currentMetaData != null) {
                            currentMetaData.setCas(currentCas);
                        }
                        break;
                    case 't':
                        if (currentMetaData != null) {
                            currentMetaData.setSecondsLeftToExpire(Integer.parseInt(value));
                        }
                        break;
                    case 's':
                        if (currentMetaData != null) {
                            currentMetaData.setSizeInBytes(Integer.parseInt(value));
                        }
                        break;
                    case 'l':
                        if (currentMetaData != null) {
                            currentMetaData.setSecondsSinceLastAccess(Long.parseLong(value));
                        }
                        break;
                }
            }
        }
    }

    @Override
    public void handleRead(ByteBuffer b) {
        if (currentData == null) return;
        
        // If we're not looking for termination, we're still reading data
        if (lookingFor == '\0') {
            int toRead = currentData.length - readOffset;
            int available = b.remaining();
            toRead = Math.min(toRead, available);
            
            if (log.isDebugEnabled()) {
                log.debug("Reading {} bytes for key {}", toRead, currentKey);
            }
            
            b.get(currentData, readOffset, toRead);
            readOffset += toRead;
        }
        
        // Check if we've read all data
        if (readOffset == currentData.length && lookingFor == '\0') {
            // Create EVCacheItem with data and metadata
            EVCacheItem<Object> item = new EVCacheItem<>();
            item.setData(currentData);
            item.setFlag(currentFlags);
            copyMetadata(item.getItemMetaData(), currentMetaData);
            
            cb.gotData(currentKey, item);
            lookingFor = '\r';
        }
        
        // Handle line termination
        if (lookingFor != '\0' && b.hasRemaining()) {
            do {
                byte tmp = b.get();
                assert tmp == lookingFor : "Expecting " + lookingFor + ", got " + (char) tmp;
                
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
            
            // Reset for next value
            if (lookingFor == '\0') {
                currentData = null;
                currentKey = null;
                currentMetaData = null;
                readOffset = 0;
                setReadType(OperationReadType.LINE);
            }
        }
    }

    private void copyMetadata(EVCacheItemMetaData dest, EVCacheItemMetaData src) {
        if (dest != null && src != null) {
            dest.setCas(src.getCas());
            dest.setSecondsLeftToExpire(src.getSecondsLeftToExpire());
            dest.setSecondsSinceLastAccess(src.getSecondsSinceLastAccess());
            dest.setSizeInBytes(src.getSizeInBytes());
            dest.setSlabClass(src.getSlabClass());
            dest.setHasBeenFetchedAfterWrite(src.isHasBeenFetchedAfterWrite());
        }
    }

    @Override
    public void initialize() {
        // Meta get supports multiple keys in single command: mg <key1> <key2> ... <flags>*\r\n
        List<String> flags = new ArrayList<>();
        
        // Add metadata flags based on config
        if (config.isIncludeTtl()) flags.add("t");         // Return TTL
        if (config.isIncludeCas()) flags.add("c");         // Return CAS token
        if (config.isIncludeSize()) flags.add("s");        // Return item size  
        if (config.isIncludeLastAccess()) flags.add("l");  // Return last access time
        
        // Add behavioral flags per meta protocol spec
        if (config.isServeStale()) {
            flags.add("R" + config.getMaxStaleTime());      // Recache flag with TTL threshold
        }
        
        // Always include client flags and value
        flags.add("f");  // Return client flags
        flags.add("v");  // Return value data
        
        // Build command: mg key1 key2 key3 f v c t s\r\n
        StringBuilder cmdBuilder = new StringBuilder();
        cmdBuilder.append("mg");
        
        // Add all keys
        for (String key : config.getKeys()) {
            cmdBuilder.append(" ").append(key);
        }
        
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
        return "Cmd: mg Keys: " + config.getKeys().size();
    }
}