package org.kin.hbase.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class HBaseConnection implements Connection {
    private Connection connection;
    private HBasePool pool;

    public HBaseConnection(Connection connection) {
        this.connection = connection;
    }

    public HBaseConnection(Connection connection, HBasePool pool) {
        this.connection = connection;
        this.pool = pool;
    }

    @Override
    public Configuration getConfiguration() {
        if (connection != null) {
            return connection.getConfiguration();
        }

        return null;
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
        if (connection != null) {
            return connection.getTable(tableName);
        }

        return null;
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService executorService) throws IOException {
        if (connection != null) {
            return connection.getTable(tableName, executorService);
        }

        return null;
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        if (connection != null) {
            return connection.getBufferedMutator(tableName);
        }

        return null;
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams) throws IOException {
        if (connection != null) {
            return connection.getBufferedMutator(bufferedMutatorParams);
        }

        return null;
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        if (connection != null) {
            return connection.getRegionLocator(tableName);
        }

        return null;
    }

    @Override
    public void clearRegionLocationCache() {
        connection.clearRegionLocationCache();
    }

    @Override
    public Admin getAdmin() throws IOException {
        if (connection != null) {
            return connection.getAdmin();
        }

        return null;
    }

    @Override
    public void close() throws IOException {
        if (pool != null) {
            pool.recycle(this);
        } else {
            connection.close();
        }

        connection = null;
        pool = null;
    }

    @Override
    public boolean isClosed() {
        if (connection != null) {
            return connection.isClosed();
        }

        return true;
    }

    @Override
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService executorService) {
        return connection.getTableBuilder(tableName, executorService);
    }

    @Override
    public void abort(String s, Throwable throwable) {
        if (connection != null) {
            connection.abort(s, throwable);
        }
    }

    @Override
    public boolean isAborted() {
        if (connection != null) {
            return connection.isAborted();
        }

        return true;
    }

    public boolean isSamePool(HBasePool oPool) {
        if (connection != null && pool != null) {
            return pool == oPool;
        }

        return false;
    }

    //getter
    Connection getConnection() {
        return connection;
    }
}
