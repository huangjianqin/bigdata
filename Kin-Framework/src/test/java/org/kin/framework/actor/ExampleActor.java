package org.kin.framework.actor;

import org.kin.framework.actor.domain.ActorPath;
import org.kin.framework.actor.domain.PoisonPill;
import org.kin.framework.actor.impl.AbstractActor;
import org.kin.framework.actor.impl.ActorSystem;
import org.kin.framework.actor.impl.ReceiveBuilder;

/**
 * Created by huangjianqin on 2018/6/5.
 */
public class ExampleActor extends AbstractActor<ExampleActor> {

    public ExampleActor(ActorPath actorPath, ActorSystem actorSystem) {
        super(actorPath, actorSystem);
    }

    @Override
    public Receive createReceiver() {
        return ReceiveBuilder.create().match(Integer.class, (applier, message) -> {
            System.out.println(applier.getClass().getName());
            System.out.println(message);
        }).build();
    }

    @Override
    public int compareTo(ExampleActor o) {
        return 0;
    }

    @Override
    protected void preStart() {
        System.out.println("pre start");
    }

    @Override
    protected void postStart() {
        System.out.println("post start");
    }

    @Override
    protected void preStop() {
        System.out.println("pre stop");
    }

    @Override
    protected void postStop() {
        System.out.println("post stop");
    }

    public static void main(String[] args) {
        ActorSystem actorSystem = ActorSystem.create();
        ExampleActor exampleActor = actorSystem.actorOf(ExampleActor.class, "aaa");
        exampleActor.tell(actor -> System.out.println("1"));
        exampleActor.receive(2);
        exampleActor.receive(PoisonPill.instance());
    }
}
