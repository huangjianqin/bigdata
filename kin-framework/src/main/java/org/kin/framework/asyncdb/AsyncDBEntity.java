package org.kin.framework.asyncdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by huangjianqin on 2019/3/31.
 * 支持多线程操作
 */
public abstract class AsyncDBEntity implements Serializable{
    private static final Logger log = LoggerFactory.getLogger("asyncDB");

    private volatile AtomicReference<DBStatus> status = new AtomicReference<>(DBStatus.NORMAL);
    private volatile DBSynchronzier DBSynchronzier;

    public void insert(){
        AsyncDBService.getInstance().dbOpr(this, DBOperation.Insert);
    }
    public void update(){
        AsyncDBService.getInstance().dbOpr(this, DBOperation.Update);
    }
    public void delete(){
        AsyncDBService.getInstance().dbOpr(this, DBOperation.Delete);
    }

    protected void serialize(){
        //do nothing, waitting to overwrite
    }

    protected void deserialize(){
        //do nothing, waitting to overwrite
    }

    boolean isCanPersist(DBOperation operation){
        DBStatus now;
        do {
            now = status.get();
            if (!operation.isCanTransfer(now)) {
                throw new AsyncDBException("DB操作失败 -> " + toString() + " - " + now + " - " + operation);
            }
        } while (!status.compareAndSet(now, operation.getTargetStauts()));
        return now == DBStatus.NORMAL;
    }


    boolean tryBDOpr(int tryTimes){
        DBStatus now;
        do {
            now = status.get();
        } while (!status.compareAndSet(now, now == DBStatus.DELETED ? DBStatus.DELETED : DBStatus.NORMAL));


        int nowTry = 0;
        do {
            if (nowTry++ >= tryTimes) {
                return false;

            }
        } while (!now.execute(DBSynchronzier, this));

        return true;
    }

    //setter && getter
    DBSynchronzier getAsyncPersistent() {
        return DBSynchronzier;
    }

    void setAsyncPersistent(DBSynchronzier DBSynchronzier) {
        this.DBSynchronzier = DBSynchronzier;
    }
}
