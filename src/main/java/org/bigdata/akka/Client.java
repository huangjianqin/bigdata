package org.bigdata.akka;

import akka.actor.*;

/**
 * Created by 健勤 on 2017/5/18.
 */
public class Client extends UntypedActor {
    private static int count = 0;

    @Override
    public void preStart() throws Exception {
        ActorSystem actorSystem = getContext().system();
        System.out.println(self().path());

        if(count < 5){
            getContext().actorOf(Props.create(Client.class), "client" + count);
            count++;
        }

        super.preStart();
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof PoisonPill){
            System.out.println("worker shutdown");
        }
        else {
            System.out.println("client receive:" + message.toString());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ActorSystem actorSystem = ActorSystem.create("akkaTest");
        ActorRef client = actorSystem.actorOf(Props.create(Client.class), "client");
        ActorRef server = actorSystem.actorOf(Props.create(Server.class), "server");
        server.tell("haha", client);
        Thread.sleep(5000);
        client.tell(PoisonPill.getInstance(), client);
        server.tell(PoisonPill.getInstance(), server);


    }
}
