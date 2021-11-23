package org.kin.jraft.counter;

import org.kin.framework.log.LoggerOprs;
import org.kin.jraft.RaftService;

/**
 * @author huangjianqin
 * @date 2021/11/8
 */
public interface CounterRaftService extends RaftService, LoggerOprs {
    /**
     * 获取counter当前值
     *
     * @param readOnlySafe 是否提供一致性读服务
     */
    void get(boolean readOnlySafe, CounterClosure closure);

    /**
     * 给counter增加指定值
     */
    void incrementAndGet(long delta, CounterClosure closure);
}
