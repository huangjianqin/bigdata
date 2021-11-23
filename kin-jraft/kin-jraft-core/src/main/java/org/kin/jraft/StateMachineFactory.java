package org.kin.jraft;

import javax.annotation.Nullable;

/**
 * {@link com.alipay.sofa.jraft.StateMachine}工厂
 *
 * @author huangjianqin
 * @date 2021/11/7
 */
@FunctionalInterface
public interface StateMachineFactory<NW extends DefaultStateMachine, S extends RaftService> {
    /**
     * 创建{@link com.alipay.sofa.jraft.StateMachine}实例并进行初始化
     *
     * @param bootstrap   raft node bootstrap
     * @param raftService raft service
     */
    NW create(RaftServerBootstrap bootstrap, @Nullable S raftService);
}
