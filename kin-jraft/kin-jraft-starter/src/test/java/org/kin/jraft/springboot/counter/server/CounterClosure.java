package org.kin.jraft.springboot.counter.server;

import org.kin.jraft.AbstractClosure;
import org.kin.jraft.springboot.counter.message.ValueResponse;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public abstract class CounterClosure extends AbstractClosure<ValueResponse> {
    /** counter 操作类型 */
    private CounterOperation operation;

    protected void success(long value) {
        success();
        response.setValue(value);
    }

    @Override
    protected ValueResponse createResponse() {
        return new ValueResponse();
    }

    //setter && getter
    public void setOperation(CounterOperation operation) {
        this.operation = operation;
    }

    public CounterOperation getOperation() {
        return operation;
    }
}
