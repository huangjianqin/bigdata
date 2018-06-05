package org.kin.framework.actor;

import org.kin.framework.actor.impl.AbstractActor;

/**
 * Created by huangjianqin on 2018/6/5.
 *
 * 处理预定义方法匹配接口
 */
public interface Receive {
    interface Func<AA extends AbstractActor<AA>, T>{
        void apply(AA applier, T message) throws Exception;
    }

    <AA extends AbstractActor<AA>, T> void receive(AA applier, T message);
}
