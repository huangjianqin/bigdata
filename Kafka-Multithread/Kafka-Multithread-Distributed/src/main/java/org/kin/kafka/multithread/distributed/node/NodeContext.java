package org.kin.kafka.multithread.distributed.node;

/**
 * Created by huangjianqin on 2017/9/19.
 */
public class NodeContext {
    private long nodeId;
    private int protocolPort;
    private String host;

    public NodeContext() {
    }

    public NodeContext(String host, long nodeId, int protocolPort) {
        this.host = host;
        this.nodeId = nodeId;
        this.protocolPort = protocolPort;
    }

    public long getNodeId() {
        return nodeId;
    }

    public int getProtocolPort() {
        return protocolPort;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
