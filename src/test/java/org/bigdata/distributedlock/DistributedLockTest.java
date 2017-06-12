package org.bigdata.distributedlock;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by 健勤 on 2017/5/25.
 */
public class DistributedLockTest {
    public static Long counter = 0L;
    public static void main(String[] args) throws InterruptedException {
        int para = 50;
        ExecutorService executor = Executors.newFixedThreadPool(para);
        CountDownLatch latch = new CountDownLatch(para);
        for(int i = 0; i < para; i++){
          executor.submit(new SelfIncrementThread(latch));
        }
        System.out.println("starting...");
        while(latch.getCount() > 0){
            System.out.println("当前Counter = " + DistributedLockTest.counter);
            Thread.sleep(20000);
        }
        latch.await();
        System.out.println("self increment end!");
        System.out.println(counter);
        System.out.println(counter == para * 10);
        executor.shutdown();
    }
}

class SelfIncrementThread implements Runnable{
//    private DistributedLock lock = new RedisDistributedLock("139.199.185.84", "self-increment");
    private DistributedLock lock = new ZKDistributedLock("139.199.185.84", "self-increment");
    private CountDownLatch latch;

    public SelfIncrementThread(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void run() {
        lock.init();
        Random random = new Random();
        for(int i = 0; i < 10; i++){
            lock.lock();
            DistributedLockTest.counter++;
            try {
                Thread.sleep(random.nextInt(1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lock.unlock();
        }
        lock.destroy();
        latch.countDown();
    }
}
