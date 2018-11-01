package org.kin.bigdata.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;

/**
 * Created by 健勤 on 2017/5/18.
 */
public class Server extends AbstractActor {
    private ActorRef worker;

    @Override
    public void preStart() throws Exception {
        worker = getContext().actorOf(Props.create(Worker.class), "worker");
        super.preStart();
    }

    @Override
    public void postStop() throws Exception {
        worker.tell(PoisonPill.getInstance(), self());
        System.out.println("server shutdown");
        super.postStop();
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(String.class, message -> {
                    String content = message.toString();
                    System.out.println(content);
                    if (content.equals("a")) {
                        getContext().actorSelection("akka://akkaTest/user/client").tell("success", self());
                    } else {
                        worker.tell(message, self());
                    }
                })
                .match(PoisonPill.class, message -> {
                    System.out.println("server shutdown");
                })
                .build();
    }
}
