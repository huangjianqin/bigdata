package org.kin.jraft.counter;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import org.kin.jraft.DefaultStateMachine;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public class CounterStateMachine extends DefaultStateMachine<Long> {
    /**
     * Counter value
     */
    private final AtomicLong value = new AtomicLong(0);

    public CounterStateMachine() {
    }

    @Override
    public void onApply(Iterator iterator) {
        while (iterator.hasNext()) {
            long current = 0;
            CounterOperation counterOperation = null;

            CounterClosure closure = null;
            if (iterator.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                // fast
                closure = (CounterClosure) iterator.done();
                counterOperation = closure.getOperation();
            } else {
                // 手动序列化
                ByteBuffer data = iterator.getData();
                try {
                    // TODO: 2021/11/14 新增支持protobuff 
                    counterOperation = SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                            data.array(), CounterOperation.class.getName());
                } catch (CodecException e) {
                    error("fail to decode IncrementAndGetRequest", e);
                }
            }
            if (counterOperation != null) {
                switch (counterOperation.getOp()) {
                    case CounterOperation.GET:
                        current = value.get();
                        info("Get value={} at logIndex={}", current, iterator.getIndex());
                        break;
                    case CounterOperation.INCREMENT:
                        long delta = counterOperation.getDelta();
                        long prev = value.get();
                        current = value.addAndGet(delta);
                        info("Added value={} by delta={} at logIndex={}, current = {}", prev, delta, iterator.getIndex(), current);
                        break;
                }

                if (closure != null) {
                    closure.success(current);
                    closure.run(Status.OK());
                }
            }
            iterator.next();
        }
    }

    public long getValue() {
        return this.value.get();
    }

    @Override
    protected Long getSnapshotValue() {
        return getValue();
    }

    @Override
    protected void onSnapshotLoad(Long value) {
        this.value.set(value);
    }
}
