package org.kin.hbase.core.op.impl;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.kin.hbase.core.HBasePool;
import org.kin.hbase.core.op.AbstractHBaseOp;
import org.kin.hbase.core.utils.HBaseUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class PutOp extends AbstractHBaseOp<PutOp> {
    public PutOp(String tableName) {
        super(tableName);
    }

    public <T> void put(T... entities) {
        put0(HBaseUtils.convert2Puts(entities));
    }

    public <T> void put(Collection<T> entities) {
        put0(HBaseUtils.convert2Puts(entities));
    }

    private void put0(List<Put> puts) {
        if (puts == null || puts.size() <= 0) {
            return;
        }

        try (Connection connection = HBasePool.common().getConnection()) {
            Table table = connection.getTable(TableName.valueOf(getTableName()));

            table.put(puts);

            table.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}
