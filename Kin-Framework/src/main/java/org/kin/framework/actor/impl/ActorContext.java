package org.kin.framework.actor.impl;

import org.kin.framework.actor.Message;
import org.kin.framework.actor.domain.ActorPath;
import org.kin.framework.actor.domain.PoisonPill;
import org.kin.framework.actor.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by huangjianqin on 2018/6/5.
 */
public class ActorContext<AA extends AbstractActor<AA>> implements Runnable{
    private static final Logger log = LoggerFactory.getLogger("Actor");
    private static final Logger profileLog = LoggerFactory.getLogger("ActorProfile");

    //唯一标识该ActorSystem下的这个Actor
    private final ActorPath actorPath;
    private final AA self;
    private final Receive receive;
    private final ActorSystem actorSystem;

    private Queue<Mail<AA>> mailBox = new ConcurrentLinkedDeque<>();
    private AtomicInteger boxSize = new AtomicInteger();
    private volatile Thread currentThread;

    private static Map<ActorContext<?>, Queue<Future>> futures = new ConcurrentHashMap<>();
    static {
        ForkJoinPool.commonPool().execute(() -> clearFinishedFutures());
    }
    //-----------------------------------------------------------------------------------------------
    ActorContext(ActorPath actorPath, AA self, Receive receive, ActorSystem actorSystem) {
        this.actorPath = actorPath;
        this.self = self;
        this.receive = receive;
        this.actorSystem = actorSystem;
    }

    @Override
    public void run() {
        this.currentThread = Thread.currentThread();
        while(true){
            Mail<AA> mail = mailBox.poll();
            if(mail == null){
                break;
            }

            long st = System.currentTimeMillis();
            mail.handle(self);
            long cost = System.currentTimeMillis() - st;

            profileLog.info("handle mail({}) cost {} ms", mail.name(), cost);

            if(boxSize.decrementAndGet() <= 0){
                break;
            }
        }
    }

    //-----------------------------------------------------------------------------------------------
    private interface Mail<AA extends AbstractActor<AA>>{
        void handle(AA applier);
        String name();
    }

    private class ReceiveMailImpl<T> implements Mail<AA>{
        private T arg;

        private ReceiveMailImpl(T arg) {
            this.arg = arg;
        }

        @Override
        public void handle(AA applier) {
            receive.receive(applier, arg);
            if(arg instanceof PoisonPill){
                //执行完用户自定义方法后, clear
                applier.stop();
            }
        }

        @Override
        public String name() {
            return "method with arg(" + arg.getClass().getName() + ")";
        }
    }

    private class MessageMailImpl implements Mail<AA>{
        private Message<AA> message;

        public MessageMailImpl(Message<AA> message) {
            this.message = message;
        }

        @Override
        public void handle(AA applier) {
            message.handle(applier);
        }

        @Override
        public String name() {
            return message.getClass().getName();
        }
    }

    //-----------------------------------------------------------------------------------------------
    private void tryRun(){
        if(boxSize.incrementAndGet() == 1){
            actorSystem.getThreadManager().execute(this);
        }
    }

    public <T> void receive(T arg){
        Mail<AA> mail = new ReceiveMailImpl<T>(arg);
        mailBox.add(mail);
        tryRun();
    }

    public void receive(Message<AA> message){
        Mail<AA> mail = new MessageMailImpl(message);
        mailBox.add(mail);
        tryRun();
    }

    public Future<?> receiveSchedule(Message<AA> message, long delay, TimeUnit unit) {
        Future future = actorSystem.getThreadManager().schedule(() -> {
            receive(message);
        }, delay, unit);
        addFuture(future);
        return future;
    }


    public Future<?> receiveScheduleAtFixedRate(Message<AA> message, long initialDelay, long period, TimeUnit unit) {
        Future future = actorSystem.getThreadManager().scheduleAtFixedRate(() -> {
            receive(message);
        }, initialDelay, period, unit);
        addFuture(future);
        return future;
    }

    public void close(){
        clearFutures();
        actorSystem.remove(actorPath);
    }

    private void addFuture(Future<?> future){
        Queue<Future> queue;
        while((queue = futures.putIfAbsent(this, new ConcurrentLinkedQueue<>())) == null)
        queue.add(future);
    }

    private void clearFutures(){
        Queue<Future> old = futures.remove(this);
        if(old != null){
            for(Future future: old){
                if(!future.isDone() || !future.isCancelled()){
                    future.cancel(true);
                }
            }
        }
    }

    private static void clearFinishedFutures(){
        for(Queue<Future> queue: futures.values()){
            Iterator<Future> iterator = queue.iterator();
            while(iterator.hasNext()){
                Future future = iterator.next();
                if(future.isDone()){
                    iterator.remove();
                }
            }
        }
    }

    //getter

    public ActorPath getActorPath() {
        return actorPath;
    }
}
