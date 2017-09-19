package org.kin.kafka.multithread.distributed.container;

import org.kin.kafka.multithread.distributed.node.ContainerContext;
import org.kin.kafka.multithread.distributed.node.NodeContext;

/**
 * Created by huangjianqin on 2017/9/18.
 */
public class ContainerImpl extends Container {
    public ContainerImpl(ContainerContext containerContext, NodeContext nodeContext) {
        super(containerContext, nodeContext);
    }

    @Override
    void start() {

    }

    @Override
    void close() {

    }
}
