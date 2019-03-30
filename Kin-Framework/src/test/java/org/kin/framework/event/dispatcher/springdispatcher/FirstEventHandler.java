package org.kin.framework.event.dispatcher.springdispatcher;

import org.kin.framework.event.EventHandlerAOT;
import org.kin.framework.event.HandleEvent;
import org.kin.framework.event.dispatcher.FirstEvent;
import org.kin.framework.event.dispatcher.FirstEventType;
import org.springframework.stereotype.Component;

/**
 * Created by huangjianqin on 2019/3/30.
 */
@Component
@EventHandlerAOT
public class FirstEventHandler{
    @HandleEvent(type = FirstEventType.class)
    public void handle(FirstEvent event) {
        System.out.println("handle " + event);
    }
}
