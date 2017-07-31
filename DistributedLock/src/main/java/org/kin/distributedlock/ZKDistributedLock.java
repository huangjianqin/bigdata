package org.kin.distributedlock;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * Created by 健勤 on 2017/5/27.
 */

/**
 * 仅支持阻塞锁和超时锁+进程故障会自动释放锁(自身特性提供)
 */
public class ZKDistributedLock implements DistributedLock {
    private static Logger log = LoggerFactory.getLogger(ZKDistributedLock.class);
    //每轮锁请求的间隔
    private static final long LOCK_REQUEST_DURATION = 300;
    //zkClient客户端
    private ZooKeeper zkClient;
    //zkClient客户端 会话超时时间
    private static final int sessionTimeout = Integer.MAX_VALUE;
    //锁节点的父节点路径名
    private static final String DL_PARENT = "/distributedlock";
    //请求的锁名字
    private String lockName;
    //redis服务器的host和port
    private String host;
    //表示该锁是否被锁上
    private boolean isLocked = false;

    public ZKDistributedLock(String host, String lockName) {
        this.lockName = lockName;
        this.host = host;
    }

    /**
     * 初始化zk客户端
     */
    @Override
    public void init() {
        final CountDownLatch latch = new CountDownLatch(1);
        try {
            this.zkClient = new ZooKeeper(host, sessionTimeout, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    if(watchedEvent.getState() == Event.KeeperState.SyncConnected){
                        latch.countDown();
                    }
                }
            });
            latch.await();
            log.info(Thread.currentThread().getName() + " >>> zk客户端连接成功");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭zk客户端
     */
    @Override
    public void destroy() {
        try {
            zkClient.close();
            log.info(Thread.currentThread().getName() + " >>> 关闭zk客户端");
        } catch (InterruptedException e) {
            log.error(Thread.currentThread().getName() + "'s lock is interrupted when destroying");
        }
    }

    /**
     * 当锁节点的父节点不存在时,创建其父节点(全局默认只有一级父节点)
     */
    private void initParent(){
        try {
            zkClient.create(DL_PARENT, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
            log.warn(Thread.currentThread().getName() + " is interrupted when operating");
        }
    }

    /**
     * 删除zk path
     * @param path
     */
    private void deleteZKPath(String path){
        try {
            zkClient.delete(path, -1);
        } catch (InterruptedException e) {
            log.warn(Thread.currentThread().getName() + " is interrupted when operating");
        } catch (KeeperException e) {
            if(e instanceof KeeperException.NoNodeException){
                //也就是存在bug了
                log.error(Thread.currentThread().getName() + " >>> 删除一个不存在的节点");
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

    public boolean requestLock(long expireTime){
        String path = DL_PARENT + "/" + lockName;
        if(!Thread.currentThread().isInterrupted()){
            //如果超时时间等于Long.MAX_VALUE,也就是阻塞锁
            if(expireTime != Long.MAX_VALUE && System.currentTimeMillis() > expireTime){
                return false;
            }
            try {
                //如果节点已存在(也就是有进程持有锁)将抛出异常
                zkClient.create(path,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                log.debug(Thread.currentThread().getName() + " >>> 获得锁");
                return true;
            } catch (KeeperException e) {
                if(e instanceof KeeperException.NoNodeException){
                    log.warn(Thread.currentThread().getName() + " >>> 不存在父节点");
                    log.info(Thread.currentThread().getName() + " 创建父节点");
                    //不存在父节点,先初始化父节点
                    initParent();
                    //再次尝试获得锁
                    return requestLock(expireTime);
                }
                else if(e instanceof KeeperException.NodeExistsException){
                    //有进程持有锁,重新尝试获得锁
                    log.debug(Thread.currentThread().getName() + " >>> 有进程持有锁");
                    try {
                        Thread.sleep(LOCK_REQUEST_DURATION);
                    } catch (InterruptedException e1) {
                        log.warn(Thread.currentThread().getName() + " is interrupted when operating");
                    }
                    return requestLock(expireTime);
                }
                else{
                    e.printStackTrace();
                    return false;
                }
            } catch (InterruptedException e) {
                log.warn(Thread.currentThread().getName() + " is interrupted when operating");
                return false;
            }
        }

        return false;
    }

    /**
     * 可中断阻塞锁
     * @throws InterruptedException
     */
    @Override
    public void lockInterruptibly() throws InterruptedException {
        if(Thread.currentThread().isInterrupted()){
            throw new InterruptedException();
        }
        lock();
    }

    /**
     * 尝试一次去获得锁,并返回结果
     * @return
     */
    @Override
    public boolean tryLock() {
        String path = DL_PARENT + "/" + lockName;
        try {
            //如果节点已存在(也就是有进程持有锁)将抛出异常
            zkClient.create(path,
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            return true;
        } catch (KeeperException e) {
            return false;
        } catch (InterruptedException e) {
            log.warn(Thread.currentThread().getName() + " is interrupted when operating");
            return false;
        }
    }

    /**
     * 不支持超时锁
     * @param time
     * @param unit
     * @return
     * @throws InterruptedException
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return requestLock(System.currentTimeMillis() + unit.toMillis(time));
    }

    /**
     * 释放锁,也就是删除zk中$DL_PARENT/lockName
     */
    @Override
    public void unlock() {
        String path = DL_PARENT + "/" + lockName;
        deleteZKPath(path);
        log.debug(Thread.currentThread().getName() + " >>> 释放锁");
    }

    /**
     * 不支持
     * @return
     */
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException("DistributedLock Base on zkClient don't support now");
    }

}
