package net.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.operation.EVCacheItem;
import com.netflix.evcache.operation.EVCacheItemMetaData;
import net.spy.memcached.CachedData;
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
    private AtomicInteger responsesReceived = new AtomicInteger(0);

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

        // Note: Individual mg commands don't send "EN" - each response is independent
        // We need to track responses and complete when we've received all of them

        if (line.startsWith("VA ")) {
            // Value with metadata: VA <size> [metadata_flags...]
            parseBulkValue(line);
            setReadType(OperationReadType.DATA);
        } else if (line.startsWith("HD")) {
            // Hit without data (metadata only): HD [metadata_flags...]
            parseBulkHit(line);
            checkAndCompleteIfDone();
        } else if (line.startsWith("EN")) {
            // Miss/End for this key: EN k<keyname>
            parseBulkMiss(line);
            checkAndCompleteIfDone();
        } else if (line.length() == 0) {
            // Empty line - ignore
        }
    }

    private void checkAndCompleteIfDone() {
        int received = responsesReceived.get();
        int total = totalKeys.get();

        if (received >= total) {
            cb.bulkComplete(totalKeys.get(), foundKeys.get(), notFoundKeys.get());
            getCallback().receivedStatus(END);
            transitionState(OperationState.COMPLETE);
        }
    }

    private void parseBulkValue(String line) {
        String[] parts = line.split(" ");
        if (parts.length < 2) return;

        // Format: VA <size> k<key> [other_flags...]
        // The 'k' flag causes the key to be in the response
        int size = Integer.parseInt(parts[1]);
        currentData = new byte[size];
        readOffset = 0;
        lookingFor = '\0';
        currentMetaData = new EVCacheItemMetaData();
        currentKey = null;

        // Parse metadata flags starting from index 2
        // The key will be in a flag like "kmy_key_name"
        parseMetadata(null, parts, 2);
        foundKeys.incrementAndGet();
    }

    private void parseBulkHit(String line) {
        String[] parts = line.split(" ");
        if (parts.length < 1) return;

        // Format: HD k<key> [other_flags...]
        currentKey = null;
        currentMetaData = new EVCacheItemMetaData();
        parseMetadata(null, parts, 1);

        // Create EVCacheItem with null data for metadata-only hit
        EVCacheItem<Object> item = new EVCacheItem<>();
        item.setData(null);
        item.setFlag(currentFlags);
        copyMetadata(item.getItemMetaData(), currentMetaData);

        if (currentKey != null) {
            cb.gotData(currentKey, item);
        }
        foundKeys.incrementAndGet();
        responsesReceived.incrementAndGet();
    }

    private void parseBulkMiss(String line) {
        // EN means not found for this mg command
        // Format: EN k<keyname> [other_flags...]
        String[] parts = line.split(" ");

        String key = null;
        // Parse the key from the response (it's in the k flag)
        for (int i = 1; i < parts.length; i++) {
            if (parts[i].length() > 0 && parts[i].charAt(0) == 'k') {
                key = parts[i].substring(1);
                break;
            }
        }

        if (key != null) {
            cb.keyNotFound(key);
        }
        notFoundKeys.incrementAndGet();
        responsesReceived.incrementAndGet();
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
                    case 'k':
                        // Key returned in response
                        currentKey = value;
                        break;
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
            // Wrap data in CachedData so EVCacheImpl can decode it properly
            EVCacheItem<Object> item = new EVCacheItem<>();
            CachedData cachedData = new CachedData(currentFlags, currentData, Integer.MAX_VALUE);
            item.setData(cachedData);
            item.setFlag(currentFlags);
            copyMetadata(item.getItemMetaData(), currentMetaData);

            cb.gotData(currentKey, item);
            responsesReceived.incrementAndGet();
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
                // Check if we're done after processing this response
                checkAndCompleteIfDone();
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
        
        // IMPORTANT: Always add 'k' flag first to return the key in the response
        // This is critical for bulk operations to match responses to keys
        flags.add("k");  // Return key in response

        // Add metadata flags based on config
        if (config.isIncludeCas()) flags.add("c");         // Return CAS token
        if (config.isIncludeTtl()) flags.add("t");         // Return TTL
        if (config.isIncludeSize()) flags.add("s");        // Return item size
        if (config.isIncludeLastAccess()) flags.add("l");  // Return last access time

        // Add behavioral flags per meta protocol spec
        if (config.isServeStale()) {
            flags.add("R" + config.getMaxStaleTime());      // Recache flag with TTL threshold
        }

        // Always include client flags and value
        flags.add("f");  // Return client flags
        flags.add("v");  // Return value
        
        // Build commands: mg sends ONE command PER KEY, not multiple keys in one command
        // Format: mg <key> <flags>\r\n for EACH key
        StringBuilder cmdBuilder = new StringBuilder();

        for (String key : config.getKeys()) {
            cmdBuilder.append("mg ").append(key);

            // Add flags for this key
            for (String flag : flags) {
                cmdBuilder.append(" ").append(flag);
            }
            cmdBuilder.append("\r\n");
        }

        String fullCommand = cmdBuilder.toString();
        byte[] cmdBytes = fullCommand.getBytes();
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