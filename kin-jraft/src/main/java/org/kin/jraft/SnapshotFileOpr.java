package org.kin.jraft;

import java.io.IOException;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public interface SnapshotFileOpr<T> {
    /**
     * 保存快照
     */
    boolean save(String path, T obj);

    /**
     * 加载快照
     */
    T load(String path) throws IOException;
}
