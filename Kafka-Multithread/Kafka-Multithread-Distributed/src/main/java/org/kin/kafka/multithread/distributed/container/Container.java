package org.kin.kafka.multithread.distributed.container;

import org.kin.kafka.multithread.core.Application;
import org.kin.kafka.multithread.distributed.node.ContainerContext;
import org.kin.kafka.multithread.distributed.node.NodeContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by huangjianqin on 2017/9/12.
 * Application容器,多JVM运行,充分利用同一节点的计算和存储资源
 */
public abstract class Container {
    protected long containerId;
    protected long idleTimeout;
    protected long reportInternal;
    protected int containerMasterProtocolPort;
    protected int nodeMasterProtocolPort;
    //所属NodeId
    protected long belong2;

    protected Map<String, Application> appName2Application = new HashMap<>();

    protected Container(ContainerContext containerContext, NodeContext nodeContext) {
        this.idleTimeout = containerContext.getIdleTimeout();
        this.containerMasterProtocolPort = containerContext.getProtocolPort();
        this.nodeMasterProtocolPort = nodeContext.getProtocolPort();
        this.reportInternal = containerContext.getReportInternal();
    }

    abstract void start();
    abstract void close();

    public long getContainerId() {
        return containerId;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public int getContainerMasterProtocolPort() {
        return containerMasterProtocolPort;
    }

    public int getNodeMasterProtocolPort() {
        return nodeMasterProtocolPort;
    }

    public long getBelong2() {
        return belong2;
    }
}
