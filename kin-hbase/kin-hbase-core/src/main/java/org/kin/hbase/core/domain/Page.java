package org.kin.hbase.core.domain;

import java.util.List;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class Page<T> {
    private final int pageSize;
    private final int pageNo;
    private final List<T> entityList;

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
