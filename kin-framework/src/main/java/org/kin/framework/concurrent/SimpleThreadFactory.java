package org.kin.framework.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by huangjianqin on 2019/3/1.
 */
public class SimpleThreadFactory implements ThreadFactory {
    private final ThreadGroup threadGroup;
    private final AtomicInteger counter;
    private final String namePrefix;
    private final boolean daemon;

    public SimpleThreadFactory(String namePrefix) {
        this(namePrefix, false);
    }

    public SimpleThreadFactory(String namePrefix, boolean daemon) {
        this.namePrefix = namePrefix + "--thread-";
        this.daemon = daemon;
        this.counter = new AtomicInteger(1);
        SecurityManager local = System.getSecurityManager();
        this.threadGroup = local == null ? Thread.currentThread().getThreadGroup() : local.getThreadGroup();
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(this.threadGroup, r, this.namePrefix + counter.getAndIncrement(), 0L);
        if (thread.isDaemon()) {
            thread.setDaemon(this.daemon);
        }
//        if(thread.getPriority() != 5){
//            thread.setPriority(5);
//        }
        return thread;
    }
}
