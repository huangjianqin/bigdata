package org.kin.distributelock;

import java.util.concurrent.locks.Lock;

/**
 * Created by 健勤 on 2017/5/25.
 */
public interface DistributeLock extends Lock {
    default DistributeLock redis(String host, int port, String lockName) {
        RedisDistributeLock lock = new RedisDistributeLock(host, port, lockName);
        lock.init();
        return lock;
    }

    default DistributeLock redis(String host, String lockName) {
        RedisDistributeLock lock = new RedisDistributeLock(host, lockName);
        lock.init();
        return lock;
    }

    default DistributeLock zk(String address, String lockName) {
        ZKDistributeLock lock = new ZKDistributeLock(address, lockName);
        lock.init();
        return lock;
    }

    /**
     * 初始化
     */
    void init();

    /**
     * 销毁, 释放资源
     */
    void destroy();
}
