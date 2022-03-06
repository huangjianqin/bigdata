package org.kin.hbase.core;

import org.kin.hbase.core.annotation.Column;
import org.kin.hbase.core.annotation.HBaseEntity;
import org.kin.hbase.core.annotation.RowKey;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
@HBaseEntity
public class HEntity {
    @RowKey
    private String rowKey;
    @Column(family = "t1")
    private String v1;
    @Column(family = "t1")
    private String v2;

    public HEntity() {
    }

    public HEntity(String rowKey, String v1, String v2) {
        this.rowKey = rowKey;
        this.v1 = v1;
        this.v2 = v2;
    }

    //setter && getter
    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getV1() {
        return v1;
    }

    public void setV1(String v1) {
        this.v1 = v1;
    }

    public String getV2() {
        return v2;
    }

    public void setV2(String v2) {
        this.v2 = v2;
    }

    @Override
    public String toString() {
        return "HEntity{" +
                "rowKey='" + rowKey + '\'' +
                ", v1='" + v1 + '\'' +
                ", v2='" + v2 + '\'' +
                '}';
    }
}
