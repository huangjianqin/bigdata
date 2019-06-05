package org.kin.framework.event.simplelistener;

import org.kin.framework.event.impl.SimpleListenerManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by huangjianqin on 2019/3/1.
 */
@Component
public class SimpleListenerManagerTest {
    private static SimpleListenerManager listenerManager;

    public static void main(String[] args) {
        ApplicationContext applicationContext = new FileSystemXmlApplicationContext("classpath:application.xml");
        List<Listener1> listener1s = getListenerManager().getListener(Listener1.class);
        for (Listener1 listener1 : listener1s) {
            listener1.call();
        }
    }

    public static SimpleListenerManager getListenerManager() {
        return listenerManager;
    }

    @Autowired
    public void setListenerManager(SimpleListenerManager listenerManager) {
        SimpleListenerManagerTest.listenerManager = listenerManager;
    }
}









