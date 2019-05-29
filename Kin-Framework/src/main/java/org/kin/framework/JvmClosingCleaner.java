package org.kin.framework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by huangjianqin on 2019/2/28.
 * <p>
 * 用于控制jvm close时, close一些使用spring IOC容器管理的对象(主要是释放占用资源)
 */
@Component
public class JvmClosingCleaner implements ApplicationContextAware{
    private final Logger log = LoggerFactory.getLogger(JvmClosingCleaner.class);
    private List<Closeable> closeables = new CopyOnWriteArrayList<>();

    @PostConstruct
    public void waitingClose(){
        //等spring容器完全初始化后执行
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Closeable closeable : closeables) {
                long startTime = System.currentTimeMillis();
                closeable.close();
                long endTime = System.currentTimeMillis();
                log.info("{} close cost {} ms", closeable.getClass().getSimpleName(), endTime - startTime);
            }
        }));
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Map<String, Closeable> beanMap = applicationContext.getBeansOfType(Closeable.class);
        closeables.addAll(beanMap.values());
    }

    public void add(Closeable closeable){
        closeables.add(closeable);
    }
}
