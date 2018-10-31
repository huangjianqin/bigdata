package org.kin.framework.concurrent;

/**
 * Created by huangjianqin on 2017/10/26.
 */
@FunctionalInterface
public interface Partitioner<K> {
    /**
     * 获取分区
     */
    int toPartition(K key, int numPartition);
}
