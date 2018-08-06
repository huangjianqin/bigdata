package org.kin.hbase.core.config;

/**
 * Created by huangjianqin on 2018/5/24.
 */
public class HBaseConfig {

    private String zookeeperQuorum;
    private String zookeeperClientPort;

    public HBaseConfig() {
    }

    public HBaseConfig(String zookeeperQuorum, String zookeeperClientPort) {
        this.zookeeperQuorum = zookeeperQuorum;
        this.zookeeperClientPort = zookeeperClientPort;
    }

    //setter & getter
    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public void setZookeeperQuorum(String zookeeperQuorum) {
        this.zookeeperQuorum = zookeeperQuorum;
    }

    public String getZookeeperClientPort() {
        return zookeeperClientPort;
    }

    public void setZookeeperClientPort(String zookeeperClientPort) {
        this.zookeeperClientPort = zookeeperClientPort;
    }
}
