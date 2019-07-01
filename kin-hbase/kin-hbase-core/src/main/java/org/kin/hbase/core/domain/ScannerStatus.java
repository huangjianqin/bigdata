package org.kin.hbase.core.domain;

/**
 * Created by huangjianqin on 2018/5/25.
 */
public enum ScannerStatus {
    /**
     * HBASE scanner 初始状态
     */
    Init(0, "初始化"),
    /**
     * HBASE scanner 有效状态
     */
    WORKING(1, "生效中"),
    /**
     * HBASE scanner 关闭状态
     */
    CLOSED(2, "关闭"),;

    private int id;
    private String desc;

    ScannerStatus(int id, String desc) {
        this.id = id;
        this.desc = desc;
    }

    public int getId() {
        return id;
    }
}
