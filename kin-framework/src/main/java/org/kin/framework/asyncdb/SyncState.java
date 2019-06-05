package org.kin.framework.asyncdb;

/**
 * Created by huangjianqin on 2019/4/4.
 */
public class SyncState {
    private String threadName;
    private long syncNum;
    private int waittingOprNum;
    //离上次记录期间处理的DB 实体数量
    private long syncPeriodNum;

    public SyncState(String threadName, long syncNum, int waittingOprNum, long syncPeriodNum) {
        this.threadName = threadName;
        this.syncNum = syncNum;
        this.waittingOprNum = waittingOprNum;
        this.syncPeriodNum = syncPeriodNum;
    }

    //setter && getter
    public String getThreadName() {
        return threadName;
    }

    public long getSyncNum() {
        return syncNum;
    }

    public int getWaittingOprNum() {
        return waittingOprNum;
    }

    public long getSyncPeriodNum() {
        return syncPeriodNum;
    }
}
