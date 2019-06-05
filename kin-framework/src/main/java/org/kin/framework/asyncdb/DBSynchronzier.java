package org.kin.framework.asyncdb;

/**
 * Created by huangjianqin on 2019/3/31.
 *
 * 定义DB基本操作
 */
public interface DBSynchronzier<E extends AsyncDBEntity> {
    void insert(E entity);
    void update(E entity);
    void delete(E entity);
}
