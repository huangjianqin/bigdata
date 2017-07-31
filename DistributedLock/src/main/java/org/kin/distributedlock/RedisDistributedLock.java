package org.kin.distributedlock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * Created by 健勤 on 2017/5/23.
 */

/**
 * 支持阻塞锁和超时锁+进程故障会自动释放锁(利用超时实现)
 */
public class RedisDistributedLock implements DistributedLock {
    private static Logger log = LoggerFactory.getLogger(RedisDistributedLock.class);
    //每轮锁请求的间隔
    private static final long LOCK_REQUEST_DURATION = 300;
    //阻塞锁的最大超时时间
    private static final long MAX_TIMEOUT = Integer.MAX_VALUE;
    //redis客户端连接
    private Jedis jedis;
    //请求的锁名字
    private String lockName;
    //redis服务器的host和port
    private String host;
    private int port;
    //表示该锁是否被锁上
    private boolean isLocked = false;

    public RedisDistributedLock(String host, String lockName) {
        this(host, 6379, lockName);
    }

    public RedisDistributedLock(String host, int port, String lockName) {
        this.host = host;
        this.port = port;
        this.lockName = lockName;
    }

    /**
     * 获得锁的封装方法
     * 阻塞式和超时锁都基于该方法获得锁
     * @param isBlock
     * @param time
     * @param unit
     * @return
     */
    private Boolean requestLock(boolean isBlock, Long time, TimeUnit unit){
        Long start = System.currentTimeMillis();
        //阻塞时一直尝试获得锁
        //超时锁时判断获得锁的过程是否超时
        while((isBlock || System.currentTimeMillis() - start < unit.toMillis(time)) && !Thread.currentThread().isInterrupted()){
            if(!jedis.isConnected()){
                log.error(Thread.currentThread().getName() + " redis连接中断");
            }
            long now = System.currentTimeMillis();
            //如果没有进程获得锁,redis上并没有这个key,setnx就会返回1,当前进程就可以获得锁
            if(jedis.setnx(lockName, now + "," + (isBlock? MAX_TIMEOUT : unit.toMillis(time))) == 1) {
                log.debug(Thread.currentThread().getName() + "命中锁");
                isLocked = true;
                return true;
            }
            //否则,判断持有锁的进程是否超时,若是超时,则抢占锁
            else{
                String v = jedis.get(lockName);
                if(v != null){
                    long expireTime = Long.valueOf(v.split(",")[1]);
                    long lockedTime = Long.valueOf(v.split(",")[0]);
                    //该锁超时,尝试抢占
                    //对于阻塞式锁,超时时间 = MAX_TIMEOUT,一般值比较大,很难会超时
                    /**
                     * 这里会存在一个问题,假设两个进程同时判断锁超时
                     * 一个进程先获得锁,那么另外一个进程就会通过下面逻辑尝试获得锁
                     * 但是该进程执行了getset命令,该锁的值已经不是获得锁的进程设置的值
                     * 如果获得锁的进程正常释放,问题并不大,但是如果该进程挂了
                     * 那么锁真正的超时时间就长了
                     */
                    if(now - lockedTime > expireTime){
                        //尝试抢占,并校验锁有没被其他进程抢占了,也就是key对应的value改变了
                        String ov = jedis.getSet(lockName, now + "," + (isBlock? MAX_TIMEOUT:unit.toMillis(time)));
                        if(ov != null && ov.equals(v)){
                            //没有改变,抢占成功
                            log.debug(Thread.currentThread().getName() + "命中锁");
                            isLocked = true;
                            return true;
                        }
                    }
                }
            }
            //睡眠一会再重试
            try {
                Thread.sleep(LOCK_REQUEST_DURATION);
            } catch (InterruptedException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * 阻塞式获得锁
     */
    @Override
    public void lock() {
        //阻塞
        requestLock(true, null, null);
    }

    /**
     * 可中断阻塞获得锁
     * @throws InterruptedException
     */
    @Override
    public void lockInterruptibly() throws InterruptedException {
        //没经严格测试
        if(Thread.interrupted()){
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
        long now = System.currentTimeMillis();
        return jedis.setnx(lockName, now + "," + MAX_TIMEOUT) == 1? true:false;
    }

    /**
     * 超时尝试获得锁,超时后返回是否获得锁
     * @param time
     * @param unit
     * @return
     * @throws InterruptedException
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return requestLock(false, time, unit);
    }

    /**
     * 释放锁
     * 也就是删除lockName的key
     */
    @Override
    public void unlock() {
        if (isLocked) {
            log.debug(Thread.currentThread().getName() + "释放锁");
            jedis.del(lockName);
            isLocked = false;
        }
    }

    /**
     * 不支持
     * @return
     */
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException("DistributedLock Base on Redis don't support now");
    }

    /**
     * 初始化锁
     * 初始化redis客户端
     */
    @Override
    public void init() {
        this.jedis = new Jedis(host, port, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    /**
     * 销毁锁
     * 关闭redis客户端
     */
    @Override
    public void destroy() {
        this.jedis.close();
    }
}
