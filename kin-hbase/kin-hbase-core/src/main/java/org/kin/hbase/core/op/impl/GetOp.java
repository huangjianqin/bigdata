package org.kin.hbase.core.op.impl;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.kin.hbase.core.HBasePool;
import org.kin.hbase.core.op.AbstractQueryOp;
import org.kin.hbase.core.utils.HBaseUtils;

import java.io.IOException;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class GetOp extends AbstractQueryOp<GetOp> {
    private final String rowKey;

    public GetOp(String tableName, String rowKey) {
        super(tableName);
        this.rowKey = rowKey;
    }

    //-------------------------------------------------------------------------------------------------------

    /**
     * query操作
     */
    public <T> T one(Class<T> entityClaxx) {
        Result result = one();
        return HBaseUtils.convert2HBaseEntity(entityClaxx, result);
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
            log.error(e.getMessage(), e);
        }

        return null;
    }

    //getter
    public String getRowKey() {
        return rowKey;
    }
}
