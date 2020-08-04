package org.kin.hbase.core;

import org.kin.hbase.core.config.HBaseConfig;

/**
 * Created by huangjianqin on 2018/5/27.
 */
public class HBasePoolTestBase {
    static {
        HBaseConfig config = new HBaseConfig();
        config.setZookeeperQuorum("bigdata1:2181");

        HBasePool.common().initializeConnections(config);
    }
}
