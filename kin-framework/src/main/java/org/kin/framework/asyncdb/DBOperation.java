package org.kin.framework.asyncdb;

import java.util.Arrays;
import java.util.List;

/**
 * Created by huangjianqin on 2019/4/1.
 */
public enum DBOperation {
    Insert(DBStatus.INSERT, Arrays.asList(DBStatus.NORMAL, DBStatus.DELETED)),
    Update(DBStatus.UPDATE, Arrays.asList(DBStatus.INSERT, DBStatus.UPDATE)),
    Delete(DBStatus.DELETED, Arrays.asList(DBStatus.INSERT, DBStatus.UPDATE)),;

    private DBStatus targetStauts;
    private List<DBStatus> canTransfer;

    DBOperation(DBStatus targetStauts, List<DBStatus> canTransfer) {
        this.targetStauts = targetStauts;
        this.canTransfer = canTransfer;
    }

    DBStatus getTargetStauts() {
        return targetStauts;
    }

    boolean isCanTransfer(DBStatus status){
        return canTransfer.contains(status);
    }
}
