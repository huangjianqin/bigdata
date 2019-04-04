package org.kin.framework.asyncdb.impl;

import org.kin.framework.asyncdb.AsyncDBStrategy;

/**
 * Created by huangjianqin on 2019/4/3.
 */
public class DefaultAsyncDBStrategy implements AsyncDBStrategy{
    @Override
    public int getOprNum() {
        return 10;
    }

    @Override
    public int getTryTimes() {
        return 2;
    }

    @Override
    public int getDuration(int size) {
        if(size > 50){
            return 200;
        }
        return 1000;
    }
}
