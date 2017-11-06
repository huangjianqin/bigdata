package org.kin.framework.concurrent;

/**
 * Created by huangjianqin on 2017/10/26.
 */
public interface Partitioner<K> {
    int toPartition(K key, int numPartition);
}
