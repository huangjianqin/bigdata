package org.kin.hbase.starter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Created by huangjianqin on 2018/5/26.
 */
@ConfigurationProperties(prefix = "kin.hbase")
public class SpringBootHBaseConfig {
    private String zookeeperQuorum;
    private int zookeeperClientPort;

    //setter & getter
    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public void setZookeeperQuorum(String zookeeperQuorum) {
        this.zookeeperQuorum = zookeeperQuorum;
    }

    public int getZookeeperClientPort() {
        return zookeeperClientPort;
    }

    public void setZookeeperClientPort(int zookeeperClientPort) {
        this.zookeeperClientPort = zookeeperClientPort;
    }
}
