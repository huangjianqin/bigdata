package org.kin.framework.concurrent.impl;

import org.kin.framework.concurrent.Partitioner;

/**
 * Created by huangjianqin on 2018/11/5.
 * HashMap的Hash方式
 * 更高效的hash方式
 */
public class EfficientHashPartitioner<K> implements Partitioner<K> {
    @Override
    public int toPartition(K key, int numPartition) {
        int h;
        //高低位异或 目的是增加hash的复杂度
        return key == null ? 0 : (((h = key.hashCode()) ^ h >>> 16) & (numPartition - 1));
    }
}
