package org.kin.framework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by huangjianqin on 2019/2/28.
 *
 * 用于控制jvm close时, close一些使用spring IOC容器管理的对象(主要是释放占用资源)
 */
@Component
public class JvmClosingCleaner implements ApplicationListener<ContextRefreshedEvent>, ApplicationContextAware, InitializingBean {
    private final Logger log = LoggerFactory.getLogger(JvmClosingCleaner.class);
    private ApplicationContext applicationContext;
    private List<Closeable> closeables = Collections.emptyList();

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        //等spring容器完全初始化后执行
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for(Closeable closeable : closeables){
                long startTime = System.currentTimeMillis();
                closeable.close();
                long endTime = System.currentTimeMillis();
                log.info("{} close cost {} ms", closeable.getClass().getSimpleName(), endTime - startTime);
            }
        }));
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        closeables = new ArrayList<>();

        Map<String, Closeable> beanMap = applicationContext.getBeansOfType(Closeable.class);
        for(Closeable bean: beanMap.values()){
            closeables.add(bean);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
