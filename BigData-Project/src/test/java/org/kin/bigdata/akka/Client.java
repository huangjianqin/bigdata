package org.kin.bigdata.akka;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;

/**
 * Created by 健勤 on 2017/5/18.
 */
public class Client extends AbstractActor {
    private static int count = 0;

    @Override
    public void preStart() throws Exception {
        System.out.println(self().path());

        if(count < 5){
            getContext().actorOf(Props.create(Client.class), "client" + count);
            count++;
        }

        super.preStart();
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(PoisonPill.class, message -> {
                    System.out.println("worker shutdown");
                })
                .matchAny(message -> {
                    System.out.println("client receive:" + message.toString());
                })
                .build();
    }

    public static void main(String[] args) throws InterruptedException {
        ActorSystem actorSystem = ActorSystem.create("akkaTest");
        ActorRef client = actorSystem.actorOf(Props.create(Client.class), "client");
        ActorRef server = actorSystem.actorOf(Props.create(Server.class), "server");
        server.tell("haha", client);
        Thread.sleep(5000);
        client.tell(PoisonPill.getInstance(), client);
        server.tell(PoisonPill.getInstance(), server);

        actorSystem.terminate();
    }
}
