package org.kin.kafka.common;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by 健勤 on 2017/8/5.
 */
public class ExecutorTest {
    public static void main(String[] args) throws InterruptedException {
        /**
         * 验证动态修改线程池的coreSize
         */
        int addSize = 10;
        ThreadPoolExecutor threads = new ThreadPoolExecutor(2, Integer.MAX_VALUE, 60L, TimeUnit.MINUTES, new LinkedBlockingDeque<Runnable>(5));
        for(int i = 0; i < addSize; i++){
            threads.submit(new Runnable() {
                @Override
                public void run() {
                  while(true){

                  }
                }
            });
        }

        for(int i = 0; i < 5; i++){
            System.out.println(threads.getActiveCount() + ", " +
                            threads.getCorePoolSize() + ", "  +
                            threads.getLargestPoolSize() + ", "  +
                            threads.getMaximumPoolSize() + ", "  +
                            threads.getPoolSize()
            );
            Thread.sleep(1000);
        }
        System.out.println("----------------------------------------------------------");
        threads.setCorePoolSize(5 * addSize);

        for(int i = 0; i < addSize; i++){
            threads.submit(new Runnable() {
                @Override
                public void run() {
                    while (true) {

                    }
                }
            });
        }
        for(int i = 0; i < 5; i++){
            System.out.println(threads.getActiveCount() + ", " +
                            threads.getCorePoolSize() + ", "  +
                            threads.getLargestPoolSize() + ", "  +
                            threads.getMaximumPoolSize() + ", "  +
                            threads.getPoolSize()
            );
            Thread.sleep(1000);
        }
        System.out.println("----------------------------------------------------------");
        threads.shutdownNow();
        Thread.sleep(2000);
        System.out.println(threads.isShutdown());
    }
}
