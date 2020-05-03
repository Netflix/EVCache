package net.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;
import java.util.Arrays;

import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

public class ExecCmdOperationImpl extends OperationImpl implements ExecCmdOperation {

    private static final OperationStatus OK = new OperationStatus(true, "OK",
            StatusCode.SUCCESS);

    private static final OperationStatus ERROR = new OperationStatus(true, "ERROR",
            StatusCode.ERR_INTERNAL);


    private final byte[] cmd;
    public ExecCmdOperationImpl(String arg, ExecCmdOperation.Callback c) {
        super(c);
        this.cmd = (arg + "\r\n").getBytes();
    }

    @Override
    public void initialize() {
        setBuffer(ByteBuffer.wrap(cmd));
    }


    @Override
    public void handleLine(String line) {
        if (line.equals("OK")) {
            callback.receivedStatus(OK);
            transitionState(OperationState.COMPLETE);
        } else if (line.equals("ERROR")) {
            callback.receivedStatus(ERROR);
            transitionState(OperationState.COMPLETE);
        }
    }

    @Override
    protected void wasCancelled() {
      callback.receivedStatus(CANCELLED);
    }

    @Override
    public String toString() {
      return "Cmd: " + Arrays.toString(cmd);
    }

}
