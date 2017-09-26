package org.kin.kafka.multithread.common;

import org.kin.kafka.multithread.config.AppConfig;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by huangjianqin on 2017/9/26.
 */
public class DefaultThreadFactory implements ThreadFactory{
    private final String appName;
    private final String group;

    public DefaultThreadFactory(String appName, String group) {
        this.appName = appName;
        this.group = group;
    }

    private final AtomicInteger number = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(appName + "-" + group + "-" + number.getAndIncrement() + "-thread-");
            return thread;
        }
}
