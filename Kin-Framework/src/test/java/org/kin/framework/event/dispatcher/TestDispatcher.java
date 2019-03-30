package org.kin.framework.event.dispatcher;


import org.kin.framework.event.EventHandler;
import org.kin.framework.event.impl.AsyncDispatcher;

/**
 * Created by 健勤 on 2017/8/10.
 */
public class TestDispatcher {
    public static void main(String[] args) {
        AsyncDispatcher dispatcher = new AsyncDispatcher();
//        dispatcher.register(FirstEventType.class, new FirstEventHandler());
        dispatcher.register(SecondEventType.class, new SecondEventHandler());
        dispatcher.register(ThirdEventType.class, new ThirdEventHandler());

        dispatcher.serviceInit();
        dispatcher.serviceStart();
        dispatcher.getEventHandler().handle(new FirstEvent(FirstEventType.E));
        dispatcher.serviceStop();
    }
}

class FirstEventHandler implements EventHandler<FirstEvent> {

    @Override
    public void handle(FirstEvent event) {
        System.out.println("handle " + event);
    }
}

class SecondEventHandler implements EventHandler<SecondEvent> {

    @Override
    public void handle(SecondEvent event) {
        System.out.println("handle " + event);
    }
}

class ThirdEventHandler implements EventHandler<ThirdEvent> {

    @Override
    public void handle(ThirdEvent event) {
        System.out.println("handle " + event);
    }
}
