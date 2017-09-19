package org.kin.kafka.multithread.distributed.node;

/**
 * Created by huangjianqin on 2017/9/19.
 */
public class NodeContext {
    private long nodeId;
    private int protocolPort;

    public NodeContext() {
    }

    public NodeContext(long nodeId, int protocolPort) {
        this.nodeId = nodeId;
        this.protocolPort = protocolPort;
    }

    public long getNodeId() {
        return nodeId;
    }

    public int getProtocolPort() {
        return protocolPort;
    }
}
