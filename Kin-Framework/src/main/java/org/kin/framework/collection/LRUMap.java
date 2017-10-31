package org.kin.framework.collection;

import java.util.LinkedHashMap;

/**
 * Created by huangjianqin on 2017/10/28.
 */
public class LRUMap<K, V> extends LinkedHashMap<K, V> {
    private int initItemNum;

    public LRUMap(){
        super(1000, 0.8f, true);
    }

    public LRUMap(int initItemNum) {
        super(initItemNum, 0.8f, true);
        this.initItemNum = initItemNum;
    }
}
