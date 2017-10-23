package org.kin.kafka.multithread.distributed.container;

import org.kin.kafka.multithread.distributed.container.impl.ContainerImpl;
import org.kin.kafka.multithread.distributed.node.ContainerContext;
import org.kin.kafka.multithread.distributed.node.NodeContext;
import org.kin.kafka.multithread.distributed.node.config.NodeConfig;

/**
 * Created by huangjianqin on 2017/10/23.
 */
public class ContainerRunner {
    public static void main(String[] args) {
        //利用-D设置参数
        long containerId = Long.valueOf(System.getProperty("containerId"));
        int containerProtocolPort = Integer.valueOf(System.getProperty("containerProtocolPort"));
        long idleTimeout = Long.valueOf(System.getProperty(NodeConfig.CONTAINER_IDLETIMEOUT));
        long reportInternal = Long.valueOf(System.getProperty(NodeConfig.CONTAINER_HEALTHREPORT_INTERNAL));

        long nodeId = Long.valueOf(System.getProperty("nodeId"));
        int nodeProtocolPort = Integer.valueOf(System.getProperty(NodeConfig.NODE_PROTOCOL_PORT));

        ContainerContext containerContext = new ContainerContext(containerId, containerProtocolPort, idleTimeout, reportInternal);
        NodeContext nodeContext = new NodeContext(nodeId, nodeProtocolPort);

        Container container = new ContainerImpl(containerContext, nodeContext);

        container.start();
    }
}
