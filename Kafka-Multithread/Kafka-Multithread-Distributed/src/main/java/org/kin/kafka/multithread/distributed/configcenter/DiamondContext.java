package org.kin.kafka.multithread.distributed.configcenter;

/**
 * Created by huangjianqin on 2017/10/28.
 */
public class DiamondContext {
    private String host;
    private int port;

    public DiamondContext() {
    }

    public DiamondContext(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
