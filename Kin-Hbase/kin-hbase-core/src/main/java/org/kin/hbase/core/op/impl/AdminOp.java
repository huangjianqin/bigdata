package org.kin.hbase.core.op.impl;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.kin.hbase.core.HBasePool;
import org.kin.hbase.core.op.AbstractHBaseOp;
import java.io.IOException;

/**
 * Created by huangjianqin on 2018/5/25.
 */
public class AdminOp extends AbstractHBaseOp<AdminOp> {
    public AdminOp(String tableName) {
        super(tableName);
    }

    public void dropTable(){
        try(Connection connection = HBasePool.common().getConnection()){
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(getTableName());

            admin.disableTable(tableName);
            admin.deleteTable(tableName);

            admin.close();
        } catch (IOException e) {
            log.error("", e);
        }
    }

    public void disable(){
        try(Connection connection = HBasePool.common().getConnection()){
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(getTableName());

            admin.disableTable(tableName);

            admin.close();
        } catch (IOException e) {
            log.error("", e);
        }
    }

    public void create(HColumnDescriptor... families){
        try(Connection connection = HBasePool.common().getConnection()){
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(getTableName());
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            if(families.length > 0){
                for(HColumnDescriptor columnDescriptor: families){
                    descriptor.addFamily(columnDescriptor);
                }
            }

            admin.createTable(descriptor);

            admin.close();
        } catch (IOException e) {
            log.error("", e);
        }
    }

    public void enableTable(){
        try(Connection connection = HBasePool.common().getConnection()){
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(getTableName());

            admin.enableTable(tableName);

            admin.close();
        } catch (IOException e) {
            log.error("", e);
        }
    }

    public void enableTableAsync(){
        try(Connection connection = HBasePool.common().getConnection()){
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(getTableName());

            admin.enableTableAsync(tableName);

            admin.close();
        } catch (IOException e) {
            log.error("", e);
        }
    }

    public boolean tableExists(){
        boolean result = false;
        try(Connection connection = HBasePool.common().getConnection()){
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(getTableName());

            result = admin.tableExists(tableName);

            admin.close();
        } catch (IOException e) {
            log.error("", e);
        }

        return result;
    }
}
