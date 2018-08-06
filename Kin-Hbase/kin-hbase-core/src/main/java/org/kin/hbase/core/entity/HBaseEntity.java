package org.kin.hbase.core.entity;

import java.io.Serializable;

/**
 * Created by huangjianqin on 2018/5/24.
 */
@org.kin.hbase.core.annotation.HBaseEntity
public abstract class HBaseEntity implements Serializable{
    public void serialize(){
        //do nothing
    }

    public void deserialize(){
        //do nothing
    }
}