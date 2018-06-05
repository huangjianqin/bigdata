package org.kin.framework.actor.impl;

import org.kin.framework.actor.Actor;
import org.kin.framework.actor.domain.ActorPath;
import org.kin.framework.actor.Message;
import org.kin.framework.actor.Receive;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by huangjianqin on 2018/2/26.
 *
 * 业务Actor必须继承该类并根据需求实现业务逻辑
 * 每个Actor实例必须并定一个ActorSystem
 */
public abstract class AbstractActor<AA extends AbstractActor<AA>> implements Actor<AA>, Comparable<AA>{
    private ActorContext actorContext;

    public AbstractActor(ActorPath actorPath, ActorSystem actorSystem) {
        init(actorPath, actorSystem);
    }

    private final void init(ActorPath actorPath, ActorSystem actorSystem){
        //其他线程
        preStart();
        actorContext = new ActorContext(actorPath, this, createReceiver(), actorSystem);
        actorSystem.add(actorPath, this);
        postStart();
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
        return actorContext.receiveScheduleAtFixedRate(message, initialDelay, period, unit);
    }

    @Override
    public final void stop() {
        //actor 线程
        actorContext.receive(actor -> {
            preStop();
            actorContext.close();
            postStop();
        });
    }

    @Override
    public ActorPath getPath() {
        return actorContext.getActorPath();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractActor<?> that = (AbstractActor<?>) o;

        return actorContext.getActorPath().equals(that.actorContext.getActorPath());
    }

    @Override
    public int hashCode() {
        return actorContext.getActorPath().hashCode();
    }

    //-----------------------------------------------------------------------------------------------
    protected void preStart(){

    }

    protected void postStart(){

    }

    protected void preStop(){

    }

    protected void postStop(){

    }

    public abstract Receive createReceiver();
}
