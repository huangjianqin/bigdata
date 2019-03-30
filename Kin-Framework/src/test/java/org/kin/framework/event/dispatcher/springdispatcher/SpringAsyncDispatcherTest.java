package org.kin.framework.event.dispatcher.springdispatcher;

import org.kin.framework.event.dispatcher.FirstEvent;
import org.kin.framework.event.dispatcher.FirstEventType;
import org.kin.framework.event.impl.SpringAsyncDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Created by huangjianqin on 2019/3/30.
 */
@Component
public class SpringAsyncDispatcherTest {
    private static SpringAsyncDispatcher springAsyncDispatcher;

    public static void main(String[] args) {
        ApplicationContext context = new FileSystemXmlApplicationContext("classpath:application.xml");
        getSpringAsyncDispatcher().getEventHandler().handle(new FirstEvent(FirstEventType.E));
    }

    public static SpringAsyncDispatcher getSpringAsyncDispatcher() {
        return springAsyncDispatcher;
    }

    @Autowired
    public void setSpringAsyncDispatcher(SpringAsyncDispatcher springAsyncDispatcher) {
        SpringAsyncDispatcherTest.springAsyncDispatcher = springAsyncDispatcher;
    }
}
