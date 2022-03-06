package org.kin.hbase.core;

import org.kin.hbase.core.config.HBaseConfig;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class HBasePoolTestBase {
    static {
        HBaseConfig config = new HBaseConfig();
        config.setZookeeperQuorum("bigdata1:2181");

        HBasePool.common().initializeConnections(config);
    }
}
