package org.kin.kafka.multithread.distributed.container.allocator;

import org.kin.kafka.multithread.distributed.node.ContainerContext;
import org.kin.kafka.multithread.distributed.node.NodeContext;
import org.kin.kafka.multithread.domain.HealthReport;
import org.kin.kafka.multithread.protocol.distributed.ContainerMasterProtocol;

import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/19.
 */
public interface ContainerAllocator {
    void init();
    ContainerMasterProtocol containerAllocate(ContainerContext containerContext, NodeContext nodeContext);
    void updateContainerStatus(HealthReport healthReport);
    void containerClosed(long containerId);
    void close();
}
