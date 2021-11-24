package org.kin.jraft.springboot.counter.message;

import java.io.Serializable;

/**
 * 获取最新的value的请求
 *
 * @author huangjianqin
 * @date 2021/11/14
 */
public class GetValueRequest implements Serializable {
    private static final long serialVersionUID = 5155526371262361209L;
    /** 是否安全读 */
    private boolean readOnlySafe = true;

    //setter && getter
    public boolean isReadOnlySafe() {
        return readOnlySafe;
    }

    public void setReadOnlySafe(boolean readOnlySafe) {
        this.readOnlySafe = readOnlySafe;
    }
}