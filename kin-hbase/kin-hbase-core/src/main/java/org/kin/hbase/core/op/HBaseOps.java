package org.kin.hbase.core.op;

import org.kin.hbase.core.op.impl.*;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class HBaseOps {
    public static PutOp put(String tableName) {
        return new PutOp(tableName);
    }

    public static GetOp get(String tableName, String rowKey) {
        return new GetOp(tableName, rowKey);
    }

    public static ScanOp scan(String tableName) {
        return new ScanOp(tableName);
    }

    public static ScanOp scan(String tableName, String startRow, String stopRow) {
        return new ScanOp(tableName, startRow, stopRow);
    }

    public static DeleteOp delete(String tableName) {
        return new DeleteOp(tableName);
    }

    public static AdminOp admin(String tableName) {
        return new AdminOp(tableName);
    }
}
