package net.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

public class MetaDebugOperationImpl extends EVCacheOperationImpl implements MetaDebugOperation {
    private static final Logger log = LoggerFactory.getLogger(MetaDebugOperationImpl.class);

    private static final OperationStatus END = new OperationStatus(true, "EN", StatusCode.SUCCESS);
    private static final int OVERHEAD = 32;
    private final MetaDebugOperation.Callback cb;

    private final String key;

    public MetaDebugOperationImpl(String k, MetaDebugOperation.Callback cb) {
        super(cb);
        this.key = k;
        this.cb = cb;
      }

      @Override
      public void handleLine(String line) {
          if(log.isDebugEnabled()) log.debug("meta debug of {} returned {}", key, line);
          if (line.equals("EN")) {
              getCallback().receivedStatus(END);
              transitionState(OperationState.COMPLETE);
            } else {
              String[] parts = line.split(" ", 3);
              if(log.isDebugEnabled()) log.debug("Num of parts "+ parts.length);
              if(parts.length <= 2) return;
              
              String[] kvPairs = parts[2].split(" ");
              for(String kv : kvPairs) {
                  if(log.isDebugEnabled()) log.debug("kv "+ kv);
                  String[] tuple = kv.split("=",2);
                  if(log.isDebugEnabled()) log.debug("{} = {}", tuple[0], tuple[1]);
                  cb.debugInfo(tuple[0], tuple[1]);
              }
            }
        getCallback().receivedStatus(matchStatus(line, END));
        transitionState(OperationState.COMPLETE);
      }

      @Override
      public void initialize() {
        ByteBuffer b = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length + OVERHEAD);
        setArguments(b, "me", key);
        b.flip();
        setBuffer(b);
      }

      @Override
      public String toString() {
        return "Cmd: me Key: " + key;
      }

}
