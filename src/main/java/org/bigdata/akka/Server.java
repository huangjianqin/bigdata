package org.bigdata.akka;

import akka.actor.*;

/**
 * Created by 健勤 on 2017/5/18.
 */
public class Server extends UntypedActor {
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
    public void onReceive(Object message) throws Exception {
        if(message instanceof String){
            String content = message.toString();
            if(content.equals("a")){
                client.tell("success", self());
            }else{
                worker.tell(message, self());
            }
        }
        else if(message instanceof PoisonPill){
            System.out.println("server shutdown");
        }
    }

    @Override
    public void postStop() throws Exception {
        worker.tell(PoisonPill.getInstance(), self());
        System.out.println("server shutdown");
        super.postStop();
    }
}
