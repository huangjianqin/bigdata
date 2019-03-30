package org.kin.framework.event.simplelistener;

import org.kin.framework.event.Listener;
import org.springframework.stereotype.Component;

/**
 * Created by huangjianqin on 2019/3/30.
 */
@Listener(order = Listener.MAX_ORDER)
@Component
public class Listener2Impl implements Listener1 {

    @Override
    public void call() {
        System.out.println("Listener2Impl");
    }
}
