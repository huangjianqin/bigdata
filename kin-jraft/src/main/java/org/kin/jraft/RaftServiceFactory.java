package org.kin.jraft;

import com.alipay.sofa.jraft.rpc.RpcServer;

/**
 * raft service工厂接口
 *
 * @author huangjianqin
 * @date 2021/11/7
 */
@FunctionalInterface
public interface RaftServiceFactory<S extends RaftService> {
    /** {@link DefaultRaftService}工厂 */
    RaftServiceFactory<DefaultRaftService> EMPTY = (bootstrap, rpcServer) -> DefaultRaftService.INSTANCE;

    /**
     * 创建raft service实例并进行初始化(比如注册方法监听接口)
     *
     * @param bootstrap raft node bootstrap
     * @param rpcServer raft service绑定的rpc server
     */
    S create(RaftServerBootstrap bootstrap, RpcServer rpcServer);
}
