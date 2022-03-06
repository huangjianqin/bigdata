package org.kin.hbase.core.op.impl;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.kin.hbase.core.HBasePool;
import org.kin.hbase.core.op.AbstractHBaseOp;
import org.kin.hbase.core.utils.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class DeleteOp extends AbstractHBaseOp<DeleteOp> {
    public DeleteOp(String tableName) {
        super(tableName);
    }

    public <T> void delete(T... objs) {
        delete(Arrays.asList(objs));
    }

    public <T> void delete(Collection<T> objs) {
        List<Delete> deletes = new ArrayList<>();
        for (T obj : objs) {
            if (obj instanceof String) {
                //row key
                deletes.add(new Delete(Bytes.toBytes(obj.toString())));
            } else {
                //HBase Entity
                byte[] rowKeyBytes = HBaseUtils.getRowKeyBytes(obj);
                if (rowKeyBytes != null) {
                    deletes.add(new Delete(rowKeyBytes));
                }
            }
        }

        if (deletes.size() > 0) {
            delete0(deletes);
        }
    }

    private void delete0(List<Delete> deletes) {
        try (Connection connection = HBasePool.common().getConnection()) {
            Table table = connection.getTable(TableName.valueOf(getTableName()));

            table.delete(deletes);

            table.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}
