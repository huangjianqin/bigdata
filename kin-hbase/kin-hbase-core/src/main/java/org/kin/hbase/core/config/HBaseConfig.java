package org.kin.hbase.core.config;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class HBaseConfig {
    /**
     * zookeeper地址
     */
    private String zookeeperQuorum;

    public HBaseConfig() {
    }

    public static HBaseConfig of(String zookeeperQuorum) {
        HBaseConfig hBaseConfig = new HBaseConfig();
        hBaseConfig.zookeeperQuorum = zookeeperQuorum;
        return hBaseConfig;
    }

    //setter & getter
    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public void setZookeeperQuorum(String zookeeperQuorum) {
        this.zookeeperQuorum = zookeeperQuorum;
    }
}
