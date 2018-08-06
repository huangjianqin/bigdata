package org.kin.hbase.core.domain;

/**
 * Created by huangjianqin on 2018/5/25.
 */
public enum ScannerStatus {
    Init(0, "初始化"),
    WORKING(1, "生效中"),
    CLOSED(2, "关闭"),
    ;

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
