package org.kin.framework;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Created by huangjianqin on 2019/5/29.
 * 在spring容器中扫描Closeable实现类, 并添加进释放资源队列中
 */
@Component
public class SpringJvmClosingScanner implements ApplicationContextAware {
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Map<String, Closeable> beanMap = applicationContext.getBeansOfType(Closeable.class);
        JvmCloseCleaner.DEFAULT().addAll(beanMap.values());
    }
}
