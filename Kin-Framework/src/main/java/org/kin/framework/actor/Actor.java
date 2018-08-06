package org.kin.framework.actor;


import org.kin.framework.actor.domain.ActorPath;
import org.kin.framework.actor.impl.AbstractActor;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by huangjianqin on 2018/2/26.
 *
 * Actor行为抽象
 * Actor的行为分派调度由具体的业务系统自身定义
 */
public interface Actor<AA extends AbstractActor<AA>> {
    /**
     * 消息匹配对应预定义方法并执行
     */
    <T> void receive(T message);

    /**
     * 执行@message 方法
     */
    void tell(Message<AA> message);

    /**
     * 调度执行@message 方法
     */
    Future<?> schedule(Message<AA> message, long delay, TimeUnit unit);

    /**
     * 周期性调度执行@message 方法
     */
    Future<?> scheduleAtFixedRate(Message<AA> message, long initialDelay, long period, TimeUnit unit);

    /**
     * 关闭Actor, 并释放资源
     */
    void stop();

    /**
     * 获取Actor基本信息
     */
    ActorPath getPath();
}
