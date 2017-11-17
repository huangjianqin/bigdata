package org.kin.bigdata.akka;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;

/**
 * Created by 健勤 on 2017/5/18.
 */
public class Server extends AbstractActor {
    private ActorRef worker;
    private ActorRef client;

    @Override
    public void preStart() throws Exception {
        ActorSystem actorSystem = getContext().system();
        worker = actorSystem.actorOf(Props.create(Worker.class), "worker");
        client = actorSystem.actorFor("akka://akkaTest/user/client");
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
                    if(content.equals("a")){
                        client.tell("success", self());
                    }else{
                        worker.tell(message, self());
                    }
                })
                .match(PoisonPill.class, message -> {
                    System.out.println("server shutdown");
                })
                .build();
    }
}
