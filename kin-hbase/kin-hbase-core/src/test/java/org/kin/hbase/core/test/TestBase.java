package org.kin.hbase.core.test;

import org.kin.hbase.core.HBasePool;
import org.kin.hbase.core.config.HBaseConfig;

/**
 * Created by huangjianqin on 2018/5/27.
 */
public class TestBase {
    static {
        HBaseConfig config = new HBaseConfig();
        config.setZookeeperClientPort("2181");
        config.setZookeeperQuorum("192.168.1.101:2181");

        HBasePool.common().initializeConnections(config);
    }
}
