package org.kin.framework.actor.impl;

import org.kin.framework.actor.Message;
import org.kin.framework.actor.Receive;
import org.kin.framework.actor.domain.ActorPath;
import org.kin.framework.actor.domain.PoisonPill;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by huangjianqin on 2018/6/5.
 * <p>
 * 部分成员域, 在Actor 线程, lazy init
 */
class ActorContext<AA extends AbstractActor<AA>> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger("actor");
    private static final Logger profileLog = LoggerFactory.getLogger("actorProfile");

    //唯一标识该ActorSystem下的这个Actor
    private final ActorPath actorPath;
    private final AA self;
    private Receive receive;
    private final ActorSystem actorSystem;

    private final Queue<Mail<AA>> mailBox = new LinkedBlockingDeque<>();
    private final AtomicInteger boxSize = new AtomicInteger();
    private volatile Thread currentThread;
    private volatile boolean isStarted = false;
    private volatile boolean isStopped = false;

    private static Map<ActorContext<?>, Queue<Future>> futures = new ConcurrentHashMap<>();
    //-----------------------------------------------------------------------------------------------

    ActorContext(ActorPath actorPath, AA self, ActorSystem actorSystem) {
        this.actorPath = actorPath;
        this.self = self;
        this.actorSystem = actorSystem;
    }

    /**
     * Actor 线程执行
     */
    private void selfInit() {
        self.preStart();
        receive = self.createReceiver();
        self.postStart();
        isStarted = true;
        //每12h清楚已结束的调度
        receiveFixedRateSchedule(actor -> clearFinishedFutures(), 0, 1, TimeUnit.HOURS);
    }

    @Override
    public void run() {
        if (!isStarted) {
            selfInit();
        }
        this.currentThread = Thread.currentThread();

        while (isStarted && !isStopped && this.currentThread != null && !this.currentThread.isInterrupted()) {
            Mail<AA> mail = mailBox.poll();
            if (mail == null) {
                break;
            }

            long st = System.currentTimeMillis();
            mail.handle(self);
            long cost = System.currentTimeMillis() - st;

            profileLog.info("handle mail({}) cost {} ms", mail.name(), cost);

            if (boxSize.decrementAndGet() <= 0) {
                break;
            }
        }
        this.currentThread = null;
    }

    //-----------------------------------------------------------------------------------------------
    private interface Mail<AA extends AbstractActor<AA>> {
        void handle(AA applier);

        String name();
    }

    /**
     * 处理消息匹配
     */
    private class ReceiveMailImpl<T> implements Mail<AA> {
        private T arg;

        private ReceiveMailImpl(T arg) {
            this.arg = arg;
        }

        @Override
        public void handle(AA applier) {
            receive.receive(applier, arg);
            if (arg instanceof PoisonPill) {
                //执行完开发者自定消息处理后, close
                close();
            }
        }

        @Override
        public String name() {
            return "method with arg(" + arg.getClass().getName() + ")";
        }
    }

    /**
     * 直接执行task
     */
    private class MessageMailImpl implements Mail<AA> {
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
    private void tryRun() {
        if (isStarted && !isStopped && boxSize.incrementAndGet() == 1) {
            actorSystem.getThreadManager().execute(this);
        }
    }

    <T> void receive(T arg) {
        Mail<AA> mail = new ReceiveMailImpl<T>(arg);
        mailBox.add(mail);
        tryRun();
    }

    void receive(Message<AA> message) {
        Mail<AA> mail = new MessageMailImpl(message);
        mailBox.add(mail);
        tryRun();
    }

    Future<?> receiveSchedule(Message<AA> message, long delay, TimeUnit unit) {
        Future future = actorSystem.getThreadManager().schedule(() -> {
            receive(message);
        }, delay, unit);
        addFuture(future);
        return future;
    }

    Future<?> receiveFixedRateSchedule(Message<AA> message, long initialDelay, long period, TimeUnit unit) {
        Future future = actorSystem.getThreadManager().scheduleAtFixedRate(() -> {
            receive(message);
        }, initialDelay, period, unit);
        addFuture(future);
        return future;
    }

    /**
     * Actor线程执行
     */
    void close() {
        isStopped = true;
        actorSystem.remove(actorPath);
        self.preStop();
        try {
            clearFutures();
            boxSize.set(0);
            mailBox.clear();
            //help GC
            this.currentThread = null;
        } finally {
            self.postStop();
        }
    }

    /**
     * Actor线程执行
     * ps: 停止当前执行线程, 另外开
     */
    void closeNow() {
        isStopped = true;
        actorSystem.remove(actorPath);
        if (this.currentThread != null) {
            this.currentThread.interrupt();
            //help GC
            this.currentThread = null;
        }
        actorSystem.getThreadManager().execute(() -> {
            self.preStop();
            try {
                clearFutures();
                boxSize.set(0);
                mailBox.clear();
            } finally {
                self.postStop();
            }
        });
    }

    private void addFuture(Future<?> future) {
        Queue<Future> queue;
        while ((queue = futures.putIfAbsent(this, new ConcurrentLinkedQueue<>())) == null) {
            queue.add(future);
        }
    }

    private void clearFutures() {
        Queue<Future> old = futures.remove(this);
        if (old != null) {
            for (Future future : old) {
                if (!future.isDone() || !future.isCancelled()) {
                    future.cancel(true);
                }
            }
        }
    }

    private void clearFinishedFutures() {
        Queue<Future> old = futures.get(this);
        if (old != null) {
            Iterator<Future> iterator = old.iterator();
            while (iterator.hasNext()) {
                Future future = iterator.next();
                if (future.isDone()) {
                    iterator.remove();
                }
            }
        }
    }

    //getter
    ActorPath getActorPath() {
        return actorPath;
    }

    boolean isStarted() {
        return isStarted;
    }

    boolean isStopped() {
        return isStopped;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ActorContext<?> that = (ActorContext<?>) o;

        return actorPath != null ? actorPath.equals(that.actorPath) : that.actorPath == null;
    }

    @Override
    public int hashCode() {
        return actorPath != null ? actorPath.hashCode() : 0;
    }
}
