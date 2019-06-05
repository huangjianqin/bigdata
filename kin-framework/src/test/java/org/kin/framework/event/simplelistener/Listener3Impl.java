package org.kin.framework.event.simplelistener;

import org.kin.framework.event.Listener;
import org.springframework.stereotype.Component;

/**
 * Created by huangjianqin on 2019/3/30.
 */
@Listener(order = Listener.MIN_ORDER)
@Component
public class Listener3Impl extends Listener2Impl implements Listener2 {

    @Override
    public void call() {
        System.out.println("Listener3Impl");
    }

    @Override
    public void call2() {
        System.out.println("Listener3Impl-call2");
    }
}
