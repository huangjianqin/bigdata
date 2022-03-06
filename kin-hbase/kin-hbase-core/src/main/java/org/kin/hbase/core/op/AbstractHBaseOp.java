package org.kin.hbase.core.op;

import org.kin.hbase.core.domain.HBaseConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public abstract class AbstractHBaseOp<H extends AbstractHBaseOp> {
    protected static final Logger log = LoggerFactory.getLogger(HBaseConstants.HBASE_LOGGER);
    private final String tableName;

    protected AbstractHBaseOp(String tableName) {
        this.tableName = tableName;
    }


    //getter
    public String getTableName() {
        return tableName;
    }
}
