package org.kin.framework.collection;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by huangjianqin on 2017/10/28.
 */
public class LRUMap<K, V> extends LinkedHashMap<K, V> {
    private int initItemNum;
    private final int _maxItemNum;

    public LRUMap(int maxItemNum){
        this(2, maxItemNum);
    }

    public LRUMap(int initItemNum, int maxItemNum) {
        super(initItemNum, 0.8f, true);
        this.initItemNum = initItemNum;
        this._maxItemNum = maxItemNum;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > _maxItemNum;
    }
}
