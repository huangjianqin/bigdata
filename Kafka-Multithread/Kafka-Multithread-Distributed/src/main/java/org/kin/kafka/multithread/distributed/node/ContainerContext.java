package org.kin.kafka.multithread.distributed.node;

import org.kin.kafka.multithread.distributed.node.config.DefaultNodeConfig;

/**
 * Created by huangjianqin on 2017/9/18.
 * Node中缓存的Container抽象信息,
 * 包括与Container通讯的RPC接口(如果是同一jvm模式,仅仅是一个接口实例,并没有使用RPC)
 */
public class ContainerContext {
    private long containerId;
    private int protocolPort;
    private long idleTimeout = Long.valueOf(DefaultNodeConfig.DEFAULT_CONTAINER_IDLETIMEOUT);
    private long reportInternal = Long.valueOf(DefaultNodeConfig.DEFAULT_CONTAINER_HEALTHREPORT_INTERNAL);

    public ContainerContext() {
    }

    public ContainerContext(long containerId, int protocolPort) {
        this.containerId = containerId;
        this.protocolPort = protocolPort;
    }

    public ContainerContext(long containerId, int protocolPort, long idleTimeout, long reportInternal) {
        this.containerId = containerId;
        this.protocolPort = protocolPort;
        this.idleTimeout = idleTimeout;
        this.reportInternal = reportInternal;
    }

    public long getContainerId() {
        return containerId;
    }

    public int getProtocolPort() {
        return protocolPort;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public long getReportInternal() {
        return reportInternal;
    }
}
