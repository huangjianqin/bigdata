package org.kin.framework.event.simplelistener;

import org.springframework.stereotype.Component;

/**
 * Created by huangjianqin on 2019/3/30.
 */
@Component
public class Listener1Impl implements Listener1 {

    @Override
    public void call() {
        System.out.println("Listener1Impl");
    }
}
