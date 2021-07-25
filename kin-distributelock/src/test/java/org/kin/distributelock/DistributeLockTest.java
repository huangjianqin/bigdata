package org.kin.distributelock;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by 健勤 on 2017/5/25.
 */
public class DistributeLockTest {
    public static final int loop = 10;
    public static Long counter = 0L;

    public static final DistributeLock lock = new RedisDistributeLock("bigdata1", "self-increment");
//    public static final DistributedLock lock = new ZKDistributedLock("bigdata1:2181", "self-increment");

    static {
        lock.init();
    }

    public static void main(String[] args) throws InterruptedException {
        int para = 50;
        ExecutorService executor = Executors.newFixedThreadPool(para);
        CountDownLatch latch = new CountDownLatch(para);
        for (int i = 0; i < para; i++) {
            executor.submit(new SelfIncrementThread(latch));
        }
        System.out.println("starting...");
        while (latch.getCount() > 0) {
            System.out.println("当前Counter = " + DistributeLockTest.counter);
            Thread.sleep(20000);
        }
        latch.await();
        System.out.println("self increment end!");
        System.out.println(counter);
        System.out.println(counter == para * loop);
        executor.shutdown();
        lock.unlock();
        lock.destroy();
    }
}

class SelfIncrementThread implements Runnable {
    private CountDownLatch latch;

    public SelfIncrementThread(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void run() {
        Random random = new Random();
        for (int i = 0; i < DistributeLockTest.loop; i++) {
            DistributeLockTest.lock.lock();
            DistributeLockTest.counter++;
            try {
                Thread.sleep(random.nextInt(1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            DistributeLockTest.lock.unlock();
        }
        latch.countDown();
    }
}
