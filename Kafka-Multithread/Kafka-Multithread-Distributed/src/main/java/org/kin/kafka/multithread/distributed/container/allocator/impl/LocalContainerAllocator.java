package org.kin.kafka.multithread.distributed.container.allocator.impl;

import org.kin.kafka.multithread.distributed.container.allocator.ContainerAllocator;
import org.kin.kafka.multithread.distributed.node.ContainerContext;
import org.kin.kafka.multithread.distributed.node.NodeContext;
import org.kin.kafka.multithread.protocol.distributed.ContainerMasterProtocol;

import java.util.Map;

/**
 * Created by huangjianqin on 2017/9/19.
 *
 * 同一节点,不同jvm container的分配
 * 根据container的状态进行container的选择
 */
public class LocalContainerAllocator implements ContainerAllocator {
    private Map<Long, ContainerMasterProtocol> id2Container;

    public LocalContainerAllocator(Map<Long, ContainerMasterProtocol> id2Container) {
        this.id2Container = id2Container;
    }

    @Override
    public void init() {
    }

    @Override
    public ContainerMasterProtocol containerAllocate(ContainerContext containerContext, NodeContext nodeContext) {
        return null;
    }

    @Override
    public void close() {
    }
}
