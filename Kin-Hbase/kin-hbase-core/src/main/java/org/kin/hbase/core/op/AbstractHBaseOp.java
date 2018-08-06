package org.kin.hbase.core.op;

import org.kin.hbase.core.domain.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huangjianqin on 2018/5/25.
 */
public abstract class AbstractHBaseOp<H extends AbstractHBaseOp> {
    protected static final Logger log = LoggerFactory.getLogger(Constants.HBASE_LOGGER);
    private final String tableName;

    protected AbstractHBaseOp(String tableName) {
        this.tableName = tableName;
    }


    //getter
    public String getTableName() {
        return tableName;
    }
}
