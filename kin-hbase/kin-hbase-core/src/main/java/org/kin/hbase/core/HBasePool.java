package org.kin.hbase.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.kin.framework.Closeable;
import org.kin.hbase.core.config.HBaseConfig;
import org.kin.hbase.core.domain.HBaseConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class HBasePool implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(HBaseConstants.HBASE_LOGGER);
    private static final HBasePool COMMON;

    static {
        COMMON = new HBasePool();
    }

    public HBasePool() {
        monitorJVMClose();
    }

    public static HBasePool common() {
        return COMMON;
    }

    //保存着该HBase连接池曾经初始化过的所有连接,并在销毁时,把这些连接全部关闭(不管在池中，还是借出)
    private List<Connection> initedConnections;

    private List<Connection> connections;


    public void initializeConnections(HBaseConfig... hbaseConfigs) {
        initializeConnections(Arrays.asList(hbaseConfigs));
    }

    /**
     * 每次都会重置
     */
    public void initializeConnections(List<HBaseConfig> hbaseConfigs) {
        if (hbaseConfigs == null || hbaseConfigs.size() <= 0) {
            return;
        }
        cancelAllConnections();
        connections = Collections.synchronizedList(new ArrayList<>());
        initedConnections = new ArrayList<>();

        addConnections(hbaseConfigs);
    }

    public void addConnections(Collection<HBaseConfig> hbaseConfigs) {
        for (HBaseConfig config : hbaseConfigs) {
            try {
                Configuration configuration = HBaseConfiguration.create();
                configuration.set(HConstants.ZOOKEEPER_QUORUM, config.getZookeeperQuorum());
                Connection connection = ConnectionFactory.createConnection(configuration);

                connections.add(connection);
                initedConnections.add(connection);
                log.info("add HBase cluster '{}' 's connection successfully", config.getZookeeperQuorum());
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private void cancelAllConnections() {
        if (initedConnections != null) {
            for (Connection connection : initedConnections) {
                try {
                    if (!connection.isClosed()) {
                        connection.close();
                    }
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
            connections.clear();
            initedConnections.clear();
            log.info("all HBase connections closed");
        }
    }

    /**
     * 当池中没有hbase连接时，阻塞
     */
    public Connection getConnection() {
        while (connections.size() <= 0) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }

        Connection connection = connections.remove(0);
        return new HBaseConnection(connection, this);
    }

    /**
     * 当池中没有hbase连接时，阻塞
     */
    public Connection getConnection(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        //jvm默认开启的fork join线程池
        Future<Connection> future = ForkJoinPool.commonPool().submit((Callable<Connection>) this::getConnection);
        return future.get(timeout, unit);
    }

    /**
     * 回收HBase连接
     */
    public void recycle(Connection connection) {
        if (connection instanceof HBaseConnection) {
            HBaseConnection hbaseConnection = (HBaseConnection) connection;
            if (hbaseConnection.isSamePool(this)) {
                connections.add(hbaseConnection.getConnection());
                return;
            }
        }

        try {
            if (!connection.isClosed()) {
                connection.close();
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        cancelAllConnections();
    }
}
