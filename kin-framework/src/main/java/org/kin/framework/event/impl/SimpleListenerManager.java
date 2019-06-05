package org.kin.framework.event.impl;

import org.kin.framework.event.Listener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Created by huangjianqin on 2019/3/1.
 */
@Component
public class SimpleListenerManager implements ApplicationContextAware {
    private static SimpleListenerManager defalut;

    private Map<Class<?>, List<Object>> listeners = new HashMap<>();

    public static SimpleListenerManager instance() {
        return defalut;
    }

    //setter && getter
    @Autowired
    public void setDefalut(SimpleListenerManager defalut) {
        SimpleListenerManager.defalut = defalut;
    }

    //------------------------------------------------------------------------------------------------------------------
    private void register0(Object bean) {
        Class claxx = bean.getClass();
        while (claxx != null) {
            for (Class interfaceClass : claxx.getInterfaces()) {
                if (interfaceClass.isAnnotationPresent(Listener.class)) {
                    List<Object> list = listeners.get(interfaceClass);
                    if (list == null) {
                        list = new ArrayList<>();
                        listeners.put(interfaceClass, list);
                    }
                    list.add(bean);
                }
            }
            claxx = claxx.getSuperclass();
        }
    }

    public void register(Object bean) {
        register0(bean);
        sortAll();
    }

    private int getOrder(Class<?> key, Object o) {
        Class claxx = o.getClass();
        while (claxx != null) {
            if (claxx.isAnnotationPresent(Listener.class)) {
                //子类有注解, 直接使用子类注解的order
                return ((Listener) claxx.getAnnotation(Listener.class)).order();
            }
            //继续从父类寻找@Listener注解
            claxx = claxx.getSuperclass();
        }

        //取key接口的order
        return ((Listener) key.getAnnotation(Listener.class)).order();
    }

    private void sort(Class<?> key) {
        List<Object> list = listeners.get(key);
        if (list != null && !list.isEmpty()) {
            list = new ArrayList<>(list);
            list.sort(Comparator.comparingInt(o -> -getOrder(key, o)));
            listeners.put(key, list);
        }
    }

    private void sortAll() {
        //排序
        for (Class<?> key : listeners.keySet()) {
            sort(key);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(Listener.class);
        for (Object bean : beans.values()) {
            register0(bean);
        }

        sortAll();
    }

    public  <T> List<T> getListener(Class<T> listenerClass) {
        return (List<T>) listeners.getOrDefault(listenerClass, Collections.emptyList());
    }

    //------------------------------------------------------------------------------------------------------------------
}
