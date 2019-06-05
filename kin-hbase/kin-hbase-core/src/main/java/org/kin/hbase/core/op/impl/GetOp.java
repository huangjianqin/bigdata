package org.kin.hbase.core.op.impl;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.kin.hbase.core.HBasePool;
import org.kin.hbase.core.op.QueryOp;
import org.kin.hbase.core.utils.HBaseUtils;

import java.io.IOException;

/**
 * Created by huangjianqin on 2018/5/25.
 */
public class GetOp extends QueryOp<GetOp> {
    private final String rowKey;

    public GetOp(String tableName, String rowKey) {
        super(tableName);
        this.rowKey = rowKey;
    }

    //-------------------------------------------------------------------------------------------------------
    //query操作
    public <T> T one(Class<T> entitiyClaxx) {
        Result result = one();
        return HBaseUtils.convert2HBaseEntity(entitiyClaxx, result);
    }

    private Result one() {
        try (Connection connection = HBasePool.common().getConnection()) {
            Table table = connection.getTable(TableName.valueOf(getTableName()));
            Get get = new Get(Bytes.toBytes(getRowKey()));

            HBaseUtils.setColumnAndFilter(get, getQueryInfos(), getFilters());

            Result result = table.get(get);

            table.close();

            return result;
        } catch (IOException e) {
            log.error("", e);
        }

        return null;
    }

    //getter
    public String getRowKey() {
        return rowKey;
    }
}
