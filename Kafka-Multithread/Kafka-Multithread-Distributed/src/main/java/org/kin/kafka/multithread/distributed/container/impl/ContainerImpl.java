package org.kin.kafka.multithread.distributed.container.impl;

import org.kin.kafka.multithread.distributed.container.Container;
import org.kin.kafka.multithread.distributed.node.ContainerContext;
import org.kin.kafka.multithread.distributed.node.NodeContext;
import org.kin.kafka.multithread.distributed.node.config.NodeConfig;
import org.kin.kafka.multithread.protocol.distributed.ContainerMasterProtocol;
import org.kin.kafka.multithread.protocol.distributed.NodeMasterProtocol;
import org.kin.kafka.multithread.rpc.factory.RPCFactories;
import org.kin.kafka.multithread.utils.HostUtils;

import java.util.List;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/18.
 */
public class ContainerImpl extends Container {
    private boolean isStopped = false;

    public ContainerImpl(ContainerContext containerContext, NodeContext nodeContext) {
        super(containerContext, nodeContext);
    }

    @Override
    public void doStart() {
        //启动RPC接口
        RPCFactories.serviceWithoutRegistry(ContainerMasterProtocol.class, this, containerMasterProtocolPort);
        RPCFactories.clientWithoutRegistry(NodeMasterProtocol.class, HostUtils.localhost(), nodeMasterProtocolPort);

        while(isStopped){
            //阻塞main线程......
            try {
                Thread.sleep(60 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void doClose() {
        isStopped = true;
    }

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
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                container.close();
            }
        }));

    }
}
