package org.kin.jraft;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public abstract class AbstractRaftService implements RaftService {
    protected final RaftServerBootstrap bootstrap;

    protected AbstractRaftService(RaftServerBootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }

    /**
     * 当前node是否是leader
     */
    protected boolean isLeader() {
        return bootstrap.getSm().isLeader();
    }

    /**
     * 处理非leader异常
     */
    protected void handlerNotLeaderError(AbstractClosure<?> closure) {
        closure.failure("not leader.", bootstrap.getNode().getLeaderId().toString());
        closure.run(new Status(RaftError.EPERM, "not leader"));
    }

    /**
     * leader节点apply task
     */
    protected void applyTask(Object param, AbstractClosure<?> closure) {
        if (!isLeader()) {
            handlerNotLeaderError(closure);
            return;
        }

        try {
            Task task = new Task();
            task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(param)));
            task.setDone(closure);
            bootstrap.getNode().apply(task);
        } catch (CodecException e) {
            String errorMsg = "fail to encode CounterOperation";
            error(errorMsg, e);
            closure.failure(errorMsg, StringUtils.EMPTY);
            closure.run(new Status(RaftError.EINTERNAL, errorMsg));
        }
    }
}
