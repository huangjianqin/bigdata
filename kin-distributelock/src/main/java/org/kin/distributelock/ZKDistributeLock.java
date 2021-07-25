package org.kin.distributelock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * 仅支持阻塞锁和超时锁+进程故障会自动释放锁(自身特性提供)
 * Created by 健勤 on 2017/5/27.
 */
public class ZKDistributeLock implements DistributeLock {
    private static final Logger log = LoggerFactory.getLogger(ZKDistributeLock.class);
    //每轮锁请求的间隔
    private static final long LOCK_REQUEST_DURATION = 50;
    //zk客户端 会话超时时间
    private static final int sessionTimeout = Integer.MAX_VALUE;
    //锁节点的父节点路径名
    private static final String ZK_DL_PARENT = "kin-distributedlock";

    //请求的锁名字
    private final String lockName;
    //zk address
    private final String address;

    //zk客户端
    private CuratorFramework client;

    public ZKDistributeLock(String address, String lockName) {
        this.lockName = lockName;
        this.address = address;
    }

    /**
     * 初始化zk客户端
     */
    @Override
    public void init() {
        //RetryNTimes  RetryOneTime  RetryForever  RetryUntilElapsed
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(200, 5);
        client = CuratorFrameworkFactory
                .builder()
                .connectString(address)
                .sessionTimeoutMs(sessionTimeout)
                .retryPolicy(retryPolicy)
                //根节点会多出一个以命名空间名称所命名的节点
                .namespace(ZK_DL_PARENT)
                .build();
        client.getConnectionStateListenable().addListener((curatorFramework, connectionState) -> {
            if (ConnectionState.CONNECTED.equals(connectionState)) {
                log.info("zookeeper connect created");
            } else if (ConnectionState.RECONNECTED.equals(connectionState)) {
                log.info("zookeeper reconnected");
            } else if (ConnectionState.LOST.equals(connectionState)) {
                log.warn("disconnect to zookeeper server");
            } else if (ConnectionState.SUSPENDED.equals(connectionState)) {
                log.error("connect to zookeeper server timeout '{}'", sessionTimeout);
            }
        });

        client.start();
    }

    /**
     * 关闭zk客户端
     */
    @Override
    public void destroy() {
        client.close();
        log.info(Thread.currentThread().getName() + " >>> 关闭zk客户端");
    }

    /**
     * 删除zk path
     */
    private void deleteZKPath(String path) {
        try {
            client.delete()
                    .guaranteed()
                    .forPath(path);
        } catch (InterruptedException e) {
            log.warn(Thread.currentThread().getName() + " is interrupted when operating");
        } catch (Exception e) {
            log.error("", e);
            if (e instanceof KeeperException.NoNodeException) {
                destroy();
            }
        }
    }

    /**
     * 阻塞锁
     */
    @Override
    public void lock() {
        requestLock(Long.MAX_VALUE);
    }

    public boolean requestLock(long expireTime) {
        String path = "/".concat(ZK_DL_PARENT).concat("/").concat(lockName);
        if (!Thread.currentThread().isInterrupted()) {
            //如果超时时间等于Long.MAX_VALUE,也就是阻塞锁
            if (expireTime != Long.MAX_VALUE && System.currentTimeMillis() > expireTime) {
                return false;
            }
            try {
                //如果节点已存在(也就是有进程持有锁)将抛出异常
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath(path, null);
                log.debug(Thread.currentThread().getName() + " >>> 获得锁");
                return true;
            } catch (Exception e) {
                if (e instanceof KeeperException.NodeExistsException) {
                    //有进程持有锁,重新尝试获得锁
                    log.debug(Thread.currentThread().getName() + " >>> 有进程持有锁");
                    try {
                        Thread.sleep(LOCK_REQUEST_DURATION);
                    } catch (InterruptedException e1) {

                    }
                    return requestLock(expireTime);
                } else {
                    log.error("", e);
                    return false;
                }
            }
        }

        return false;
    }

    /**
     * 可中断阻塞锁
     */
    @Override
    public void lockInterruptibly() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
        lock();
    }

    /**
     * 尝试一次去获得锁,并返回结果
     */
    @Override
    public boolean tryLock() {
        String path = "/".concat(ZK_DL_PARENT).concat("/").concat(lockName);
        try {
            //如果节点已存在(也就是有进程持有锁)将抛出异常
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(path, null);
            return true;
        } catch (InterruptedException e) {
            return false;
        } catch (Exception e) {
            log.error("", e);
            return false;
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
        return requestLock(System.currentTimeMillis() + unit.toMillis(time));
    }

    /**
     * 释放锁,也就是删除zk中$DL_PARENT/lockName
     */
    @Override
    public void unlock() {
        String path = "/".concat(ZK_DL_PARENT).concat("/").concat(lockName);
        deleteZKPath(path);
        log.debug(Thread.currentThread().getName() + " >>> 释放锁");
    }

    /**
     * 不支持
     */
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException("DistributedLock Base on zkClient don't support now");
    }

}
