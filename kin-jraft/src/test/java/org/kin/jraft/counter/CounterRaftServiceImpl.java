package org.kin.jraft.counter;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.kin.jraft.AbstractRaftService;
import org.kin.jraft.RaftServerBootstrap;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public class CounterRaftServiceImpl extends AbstractRaftService implements CounterRaftService {

    public CounterRaftServiceImpl(RaftServerBootstrap bootstrap) {
        super(bootstrap);
    }

    private long getValue() {
        CounterStateMachine sm = bootstrap.getSm();
        return sm.getValue();
    }

    @Override
    public void get(boolean readOnlySafe, CounterClosure closure) {
        if (!readOnlySafe) {
            closure.success(getValue());
            closure.run(Status.OK());
            return;
        }

        bootstrap.getNode().readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if (status.isOk()) {
                    closure.success(getValue());
                    closure.run(Status.OK());
                    return;
                }
                CounterContext.EXECUTOR.execute(() -> {
                    if (isLeader()) {
                        debug("fail to get value with 'ReadIndex': {}, try to applying to the state machine.", status);
                        applyTask(CounterOperation.createGet(), closure);
                    } else {
                        handlerNotLeaderError(closure);
                    }
                });
            }
        });
    }

    @Override
    public void incrementAndGet(long delta, CounterClosure closure) {
        CounterOperation operation = CounterOperation.createIncrement(delta);
        closure.setOperation(operation);
        applyTask(operation, closure);
    }

    @Override
    public void close() {

    }
}
