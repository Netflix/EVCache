package net.spy.memcached.protocol.ascii;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.MutatorOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.StatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

/**
 * Operation for Meta Arithmetic commands of memcached.
 */
public class MetaArithmeticOperationImpl extends EVCacheOperationImpl implements
        MutatorOperation {

    private static final Logger log = LoggerFactory.getLogger(MetaArithmeticOperationImpl.class);
    private static final OperationStatus NOT_FOUND = new OperationStatus(false,
            "NOT_FOUND", StatusCode.ERR_NOT_FOUND);

    // TODO : Move to a Builder as we expand this to support better isolation guarantees
    // Request
    private static final String META_ARITHMETIC_OP = "ma";
    private static final String AUTO_CREATE = "N%d";
    private static final String MUTATOR_MODE ="M%c";
    private static final char INCR = '+';
    private static final char DECR = '-';
    private static final String DEFAULT = "J%d";
    private static final String DELTA = "D%d";
    private static final char FLAG_VALUE = 'v';
    // Response
    private static final String VALUE_RETURN = "VA";


    private final Mutator mutator;
    private final String key;
    private final long amount;
    private final long def;
    private final int exp;
    private boolean readingValue;
    public static final int OVERHEAD = 32;

    public MetaArithmeticOperationImpl(Mutator m, String k, long amt, long defaultVal,
                                       int expiry, OperationCallback c) {
        super(c);
        mutator = m;
        key = k;
        amount = amt;
        def = defaultVal;
        exp = expiry;
        readingValue = false;
    }

    @Override
    public void handleLine(String line) {
        log.debug("Result:  %s", line);
        OperationStatus found = null;
        if (line.startsWith(VALUE_RETURN)) {
            // TODO : We may need to tokenize this when more flags are supplied to the request.
            this.readingValue = true;
            // Ask state machine to read the next line which has the response
            this.setReadType(OperationReadType.LINE);
            return;
        } else if (readingValue) {
            // TODO : Tokenize if multiple values are in this line, as of now, it's just the result.
            found = new OperationStatus(true, line, StatusCode.SUCCESS);
        } else {
            // TODO: Other NF/NS/EX and also OK are treated as errors, this will change as we extend the meta API
                found = NOT_FOUND;
        }
        getCallback().receivedStatus(found);
        transitionState(OperationState.COMPLETE);
    }

    @Override
    public void initialize() {
        int size = KeyUtil.getKeyBytes(key).length + OVERHEAD;
        ByteBuffer b = ByteBuffer.allocate(size);

        setArguments(b, META_ARITHMETIC_OP, key, String.format(AUTO_CREATE, exp),
                String.format(MUTATOR_MODE, (mutator == Mutator.incr ? INCR : DECR)),
                String.format(DEFAULT,def), String.format(DELTA,amount), FLAG_VALUE);
        b.flip();
        setBuffer(b);
    }

    public Collection<String> getKeys() {
        return Collections.singleton(key);
    }

    public long getBy() {
        return amount;
    }

    public long getDefault() {
        return def;
    }

    public int getExpiration() {
        return exp;
    }

    public Mutator getType() {
        return mutator;
    }

    @Override
    public String toString() {
        return "Cmd: " + mutator.name() + " Key: " + key + " Amount: " + amount +
                " Default: " + def +  " Expiry: " + exp;
    }
}
