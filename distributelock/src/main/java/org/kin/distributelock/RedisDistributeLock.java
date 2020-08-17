package org.kin.distributelock;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * 支持阻塞锁和超时锁+进程故障会自动释放锁(利用超时实现)
 * Created by 健勤 on 2017/5/23.
 */
public class RedisDistributeLock implements DistributeLock {
    private static final Logger log = LoggerFactory.getLogger(RedisDistributeLock.class);
    //每轮锁请求的间隔
    private static final long LOCK_REQUEST_DURATION = 50;
    //阻塞锁的最大超时时间
    private static final long MAX_TIMEOUT = Integer.MAX_VALUE;

    //请求的锁名字
    private final String lockName;
    //redis服务器的host和port
    private final String host;
    private final int port;

    //redis客户端连接
    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;

    //表示该锁是否被锁上
    private volatile long lockedThreadId;

    public RedisDistributeLock(String host, String lockName) {
        this(host, 6379, lockName);
    }

    public RedisDistributeLock(String host, int port, String lockName) {
        this.host = host;
        this.port = port;
        this.lockName = lockName;
    }

    /**
     * 初始化锁
     * 初始化redis客户端
     */
    @Override
    public void init() {
        RedisURI redisUri = RedisURI.builder()
                .withHost(host)
                .withPort(port)
                .withTimeout(Duration.of(10, ChronoUnit.SECONDS))
                .build();
        redisClient = RedisClient.create(redisUri);
        connection = redisClient.connect();
    }

    /**
     * 销毁锁
     * 关闭redis客户端
     */
    @Override
    public void destroy() {
        connection.close();
        redisClient.shutdown();
    }

    /**
     * 获得锁的封装方法
     * 阻塞式和超时锁都基于该方法获得锁
     */
    private Boolean requestLock(boolean isBlock, Long time, TimeUnit unit) {
        long start = System.currentTimeMillis();
        //阻塞时一直尝试获得锁
        //超时锁时判断获得锁的过程是否超时
        while ((isBlock || System.currentTimeMillis() - start < unit.toMillis(time))) {
            Thread currentThread = Thread.currentThread();
            RedisCommands<String, String> redisCommands = connection.sync();
            long now = System.currentTimeMillis();
            //如果没有进程获得锁,redis上并没有这个key,setnx就会返回1,当前进程就可以获得锁
            if (redisCommands.setnx(lockName, now + "," + (isBlock ? MAX_TIMEOUT : unit.toMillis(time)))) {
                log.debug(currentThread.getName() + "命中锁");
                lockedThreadId = currentThread.getId();
                return true;
            }
            //否则,判断持有锁的进程是否超时,若是超时,则抢占锁
            else {
                String v = redisCommands.get(lockName);
                if (v != null) {
                    long expireTime = Long.parseLong(v.split(",")[1]);
                    long lockedTime = Long.parseLong(v.split(",")[0]);
                    /**
                     * 该锁超时,尝试抢占
                     * 对于阻塞式锁,超时时间 = MAX_TIMEOUT,一般值比较大,很难会超时
                     *
                     * 这里会存在一个问题,假设两个进程同时判断锁超时
                     * 一个进程先获得锁,那么另外一个进程就会通过下面逻辑尝试获得锁
                     * 但是该进程执行了getset命令,该锁的值已经不是获得锁的进程设置的值
                     * 如果获得锁的进程正常释放,问题并不大,但是如果该进程挂了
                     * 那么锁真正的超时时间就长了
                     */
                    if (now - lockedTime > expireTime) {
                        //尝试抢占,并校验锁有没被其他进程抢占了,也就是key对应的value改变了
                        String ov = redisCommands.getset(lockName, now + "," + (isBlock ? MAX_TIMEOUT : unit.toMillis(time)));
                        if (ov != null && ov.equals(v)) {
                            //设置成功, 并返回原来的value, 抢占成功
                            log.debug(currentThread.getName() + "命中锁");
                            lockedThreadId = currentThread.getId();
                            return true;
                        }
                    }
                }
            }
            //睡眠一会再重试
            try {
                Thread.sleep(LOCK_REQUEST_DURATION);
            } catch (InterruptedException e) {

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
     */
    @Override
    public void lockInterruptibly() throws InterruptedException {
        //没经严格测试
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        lock();
    }

    /**
     * 尝试一次去获得锁,并返回结果
     */
    @Override
    public boolean tryLock() {
        long now = System.currentTimeMillis();
        RedisCommands<String, String> redisCommands = connection.sync();
        return redisCommands.setnx(lockName, now + "," + MAX_TIMEOUT);
    }

    /**
     * 超时尝试获得锁,超时后返回是否获得锁
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit) {
        return requestLock(false, time, unit);
    }

    /**
     * 释放锁
     * 也就是删除lockName的key
     */
    @Override
    public void unlock() {
        Thread currentThread = Thread.currentThread();
        if (currentThread.getId() == lockedThreadId) {
            log.debug(currentThread.getName() + "释放锁");
            /**
             * 先释放锁, 再删除key, 不然会存在以下情况:
             * A进程: 删除key, 但仍没有释放锁
             * B进程: 获取了分布式锁并设置锁
             * A进程: 释放锁状态
             * 待B进程要释放锁时, 却无法执行删除key的逻辑, 导致死锁
             */
            lockedThreadId = 0;

            RedisCommands<String, String> redisCommands = connection.sync();
            redisCommands.del(lockName);
        }
    }

    /**
     * 不支持
     */
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException("DistributedLock Base on Redis don't support now");
    }
}
