package org.kin.bigdata.akka;

import akka.actor.AbstractActor;
import akka.actor.PoisonPill;
import akka.japi.pf.ReceiveBuilder;

/**
 * Created by 健勤 on 2017/5/18.
 */
public class Worker extends AbstractActor{

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(String.class, message -> {
                    String content = message.toString();
                    getSender().tell("a", getSelf());
                })
                .match(PoisonPill.class, message ->{
                    System.out.println("worker shutdown");
                })
                .build();
    }
}
