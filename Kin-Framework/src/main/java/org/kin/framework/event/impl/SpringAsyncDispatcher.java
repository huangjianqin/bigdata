package org.kin.framework.event.impl;

import org.kin.framework.event.Event;
import org.kin.framework.event.EventHandler;
import org.kin.framework.event.EventHandlerAOT;
import org.kin.framework.event.HandleEvent;
import org.kin.framework.utils.ExceptionUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Created by huangjianqin on 2019/3/1.
 */
@Component
public class SpringAsyncDispatcher extends AsyncDispatcher implements ApplicationContextAware, ApplicationListener {
    private static SpringAsyncDispatcher defalut;

    //setter && getter
    public static SpringAsyncDispatcher instance() {
        return defalut;
    }

    @Autowired
    public void setDefalut(SpringAsyncDispatcher defalut) {
        SpringAsyncDispatcher.defalut = defalut;
    }

    //------------------------------------------------------------------------------------------------------------------

    /**
     * 识别@HandleEvent注解的类 or 方法, 并自动注册
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(EventHandlerAOT.class);
        for (Object bean : beans.values()) {
            Class claxx = bean.getClass();
            boolean isFindFromMethod = false;
            if (claxx.isAnnotationPresent(HandleEvent.class)) {
                HandleEvent handleEvent = (HandleEvent) claxx.getAnnotation(HandleEvent.class);
                Class eventType = handleEvent.type();
                if (eventType.isEnum()) {
                    //注解在类定义
                    if (EventHandler.class.isAssignableFrom(claxx)) {
                        EventHandler eventHandler = (EventHandler) bean;
                        register(eventType, eventHandler);
                    } else {
                        isFindFromMethod = true;
                    }
                } else {
                    throw new IllegalStateException("事件类型必须是枚举");
                }
            }
            else{
                isFindFromMethod = true;
            }

            if(isFindFromMethod){
                //注解在方法
                //在所有  public & 有注解的  方法中寻找一个匹配的方法作为事件处理方法
                for (Method method : claxx.getMethods()) {
                    if (method.isAnnotationPresent(HandleEvent.class)) {
                        HandleEvent handleEvent = (HandleEvent) method.getAnnotation(HandleEvent.class);
                        Class eventType = handleEvent.type();
                        Class[] paramClasses = method.getParameterTypes();
                        if (paramClasses.length == 1) {
                            Class paramClass = paramClasses[0];
                            if (Event.class.isAssignableFrom(paramClass) && checkEventClass(eventType, paramClass)) {
                                //满足条件
                                register(eventType, new MethodAnnotationEventHandler(bean, method, paramClass));
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        if(applicationEvent instanceof ContextStartedEvent || applicationEvent instanceof ContextRefreshedEvent){
            serviceInit();
            serviceStart();
        }
        if(applicationEvent instanceof ContextClosedEvent){
            serviceStop();
        }
    }

    //------------------------------------------------------------------------------------------------------------------
    class MethodAnnotationEventHandler<T extends Event> implements EventHandler<T> {
        private Object invoker;
        private Method method;
        private Class<T> eventClass;

        MethodAnnotationEventHandler(Object invoker, Method method, Class<T> eventClass) {
            this.invoker = invoker;
            this.method = method;
            this.eventClass = eventClass;
        }

        @Override
        public void handle(T event) {
            method.setAccessible(true);
            try {
                method.invoke(invoker, event);
            } catch (IllegalAccessException | InvocationTargetException e) {
                ExceptionUtils.log(e);
            } finally {
                method.setAccessible(false);
            }
        }

        Object getInvoker() {
            return invoker;
        }

        Method getMethod() {
            return method;
        }

        Class<T> getEventClass() {
            return eventClass;
        }
    }
}
