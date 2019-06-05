package org.kin.hbase.core.domain;

import java.util.List;

/**
 * Created by huangjianqin on 2018/5/25.
 */
public class Page<T> {
    private int pageSize;
    private int pageNo;
    private List<T> entityList;

    public Page(int pageSize, int pageNo, List<T> entityList) {
        this.pageSize = pageSize;
        this.pageNo = pageNo;
        this.entityList = entityList;
    }

    public int count() {
        return entityList.size();
    }

    //getter
    public int getPageSize() {
        return pageSize;
    }

    public int getPageNo() {
        return pageNo;
    }

    public List<T> getEntityList() {
        return entityList;
    }
}
