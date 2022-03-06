package org.kin.hbase.core.op.impl;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.kin.hbase.core.HBasePool;
import org.kin.hbase.core.op.AbstractHBaseOp;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class AdminOp extends AbstractHBaseOp<AdminOp> {
    public AdminOp(String tableName) {
        super(tableName);
    }

    public void dropTable() {
        try (Connection connection = HBasePool.common().getConnection()) {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(getTableName());

            admin.disableTable(tableName);
            admin.deleteTable(tableName);

            admin.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void disable() {
        try (Connection connection = HBasePool.common().getConnection()) {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(getTableName());

            admin.disableTable(tableName);

            admin.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void create(String... families) {
        try (Connection connection = HBasePool.common().getConnection()) {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(getTableName());
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            if (families.length > 0) {
                List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                        Arrays.stream(families).map(family -> ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build()).collect(Collectors.toList());
                tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptors);
            }
            admin.createTable(tableDescriptorBuilder.build());

            admin.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void enableTable() {
        try (Connection connection = HBasePool.common().getConnection()) {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(getTableName());

            admin.enableTable(tableName);

            admin.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void enableTableAsync() {
        try (Connection connection = HBasePool.common().getConnection()) {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(getTableName());

            admin.enableTableAsync(tableName);

            admin.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public boolean tableExists() {
        boolean result = false;
        try (Connection connection = HBasePool.common().getConnection()) {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(getTableName());

            result = admin.tableExists(tableName);

            admin.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        return result;
    }
}
