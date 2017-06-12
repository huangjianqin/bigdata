package org.bigdata.distributedlock;

import java.util.concurrent.locks.Lock;

/**
 * Created by 健勤 on 2017/5/25.
 */
public interface DistributedLock extends Lock {
    void init();
    void destroy();
}
