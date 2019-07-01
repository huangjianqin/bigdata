package org.kin.framework.actor.impl;

import org.kin.framework.actor.Actor;
import org.kin.framework.actor.Message;
import org.kin.framework.actor.Receive;
import org.kin.framework.actor.domain.ActorPath;
import org.kin.framework.actor.domain.PoisonPill;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by huangjianqin on 2018/2/26.
 * <p>
 * 业务Actor必须继承该类并根据需求实现业务逻辑
 * 每个Actor实例必须并定一个ActorSystem
 */
public abstract class AbstractActor<AA extends AbstractActor<AA>> implements Actor<AA>, Comparable<AA> {
    private ActorContext actorContext;

    public AbstractActor(ActorPath actorPath, ActorSystem actorSystem) {
        init(actorPath, actorSystem);
    }

    private final void init(ActorPath actorPath, ActorSystem actorSystem) {
        actorContext = new ActorContext(actorPath, this, actorSystem);
        actorSystem.add(actorPath, this);
    }

    @Override
    public final <T> void receive(T message) {
        actorContext.receive(message);
    }

    @Override
    public final void tell(Message<AA> message) {
        actorContext.receive(message);
    }

    @Override
    public final Future<?> schedule(Message<AA> message, long delay, TimeUnit unit) {
        return actorContext.receiveSchedule(message, delay, unit);
    }

    @Override
    public final Future<?> scheduleAtFixedRate(Message<AA> message, long initialDelay, long period, TimeUnit unit) {
        return actorContext.receiveFixedRateSchedule(message, initialDelay, period, unit);
    }

    @Override
    public final void stop() {
        actorContext.receive(PoisonPill.instance());
    }

    @Override
    public final void stopNow() {
        actorContext.closeNow();
    }

    @Override
    public ActorPath getPath() {
        return actorContext.getActorPath();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractActor<?> that = (AbstractActor<?>) o;

        return actorContext.getActorPath().equals(that.actorContext.getActorPath());
    }

    @Override
    public int hashCode() {
        return actorContext.getActorPath().hashCode();
    }

    //-----------------------------------------------------------------------------------------------

    /**
     * Actor 线程执行
     */
    protected void preStart() {

    }

    /**
     * Actor 线程执行
     */
    protected void postStart() {

    }

    /**
     * Actor 线程执行
     */
    protected void preStop() {

    }

    /**
     * Actor 线程执行
     */
    protected void postStop() {

    }

    /**
     * Actor 线程执行
     */
    public abstract Receive createReceiver();
}
