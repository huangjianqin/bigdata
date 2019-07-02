package org.kin.framework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by huangjianqin on 2019/2/28.
 * <p>
 * 用于控制jvm close时, 释放占用资源
 */
public class JvmCloseCleaner {
    private final Logger log = LoggerFactory.getLogger(JvmCloseCleaner.class);
    private Set<Closeable> closeables = new CopyOnWriteArraySet<>();
    private static final JvmCloseCleaner DEFAULT = new JvmCloseCleaner();
    static{
        DEFAULT.waitingClose();
    }

    private JvmCloseCleaner() {
    }

    private void waitingClose(){
        //等spring容器完全初始化后执行
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Closeable closeable : this.closeables) {
                log.info("{} closing...", closeable.getClass().getSimpleName());
                long startTime = System.currentTimeMillis();
                closeable.close();
                long endTime = System.currentTimeMillis();
                log.info("{} close cost {} ms", closeable.getClass().getSimpleName(), endTime - startTime);
            }
        }));
    }

    public void add(Closeable closeable){
        this.closeables.add(closeable);
    }

    public void addAll(Closeable... closeables){
        this.closeables.addAll(Arrays.asList(closeables));
    }

    public void addAll(Collection<Closeable> closeables){
        this.closeables.addAll(closeables);
    }

    public static JvmCloseCleaner DEFAULT() {
        return DEFAULT;
    }
}
