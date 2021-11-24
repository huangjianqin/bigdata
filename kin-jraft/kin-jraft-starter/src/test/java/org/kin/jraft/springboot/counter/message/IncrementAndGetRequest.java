package org.kin.jraft.springboot.counter.message;

import java.io.Serializable;

/**
 * 增加指定值并获取最新值
 *
 * @author huangjianqin
 * @date 2021/11/14
 */
public class IncrementAndGetRequest implements Serializable {
    private static final long serialVersionUID = -7290633886590313082L;
    /** 增加指定值 */
    private long delta;

    public long getDelta() {
        return this.delta;
    }

    public void setDelta(long delta) {
        this.delta = delta;
    }
}
