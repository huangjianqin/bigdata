package org.kin.bigdata.akka;

import akka.actor.PoisonPill;
import akka.actor.UntypedActor;

/**
 * Created by 健勤 on 2017/5/18.
 */
public class Worker extends UntypedActor{
    @Override
    public void onReceive(Object message) throws Exception {
        Thread.sleep(2000);
        if(message instanceof String){
            String content = message.toString();
            getSender().tell("a", self());
        }else if(message instanceof PoisonPill){
            System.out.println("worker shutdown");
        }
    }
}
