package org.kin.bigdata.event.impl;

import org.kin.bigdata.event.Dispatcher;
import org.kin.bigdata.event.Event;
import org.kin.bigdata.event.EventHandler;
import org.kin.bigdata.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by 健勤 on 2017/8/8.
 * 异步事件分发器
 */
public class AsyncDispatcher extends AbstractService implements Dispatcher {
    private static Logger log = LoggerFactory.getLogger(AsyncDispatcher.class);

    //缓存所有待分发的时间
    private final BlockingQueue<Event> eventQueue;
    //负责分发事件的线程
    private final ThreadPoolExecutor pool;
    //存储事件与其对应的事件处理器的映射
    protected final Map<Class<? extends Enum>, EventHandler> event2Dispatcher;

    //负责将事件进队的事件处理器
    private final EventHandler innerHandler = new GenericEventHandler();
    //事件分发线程
    private List<EventHandlerThread> eventHandlerThreads = new LinkedList<>();
    //事件分发线程数最大值
    private final int THREADS_LIMIT = 10;

    public AsyncDispatcher() {
        this(new LinkedBlockingQueue<Event>());
    }

    public AsyncDispatcher(BlockingQueue<Event> eventQueue){
        super("AsyncDispatcher");
        this.eventQueue = eventQueue;
        event2Dispatcher = new HashMap<>();
        pool = new ThreadPoolExecutor(1, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new InnerThreadFactory());
    }

    @Override
    public EventHandler getEventHandler() {
        return innerHandler;
    }


    @Override
    public void register(Class<? extends Enum> eventType, EventHandler handler) {
        //event2Dispatcher需同步,存在一写多读的情况
        synchronized (event2Dispatcher){
            EventHandler<Event> registered = event2Dispatcher.get(eventType);
            if(registered == null){
                event2Dispatcher.put(eventType, handler);
            }
            else if(!(registered instanceof MultiListenerHandler)){
                MultiListenerHandler multiHandler = new MultiListenerHandler();
                multiHandler.addHandler(registered);
                multiHandler.addHandler(handler);
                event2Dispatcher.put(eventType, multiHandler);
            }
            else {
                ((MultiListenerHandler)registered).addHandler(handler);
            }
        }
    }


    @Override
    public void dispatch(Event event) {
        Class<? extends Enum> type = event.getType().getDeclaringClass();
        EventHandler handler = event2Dispatcher.get(type);
        if(handler != null){
            handler.handle(event);
        }
        else{
            throw new IllegalStateException("doesn't have event handler to handle event " + type);
        }
    }


    @Override
    public void serviceInit() {
        super.serviceInit();
    }

    @Override
    public void serviceStart() {
        runNEventHandlerThread(1);
        super.serviceStart();
    }

    @Override
    public void serviceStop() {
        shutdownNEventHandlerThread(eventHandlerThreads.size());
        //用shutdownNow是为了强制中断阻塞在BlockingQueue.take()的线程
        pool.shutdownNow();

        super.serviceStop();
    }

    EventHandlerThread newThread(){
        return new EventHandlerThread();
    }

    void runNEventHandlerThread(int size){
        for(int i = 0; i < size; i++){
            EventHandlerThread thread = newThread();
            eventHandlerThreads.add(thread);
            pool.submit(thread);
        }
    }

    void shutdownNEventHandlerThread(int size){
        for(int i = 0; i < size; i++){
            eventHandlerThreads.remove(0).shutdown();
        }
    }

    /**
     * 事件处理线程,主要逻辑是从事件队列获得事件并分派出去
     */
    private final class EventHandlerThread implements Runnable{
        private boolean isStopped = false;

        @Override
        public void run() {
            while(!isStopped && !Thread.currentThread().isInterrupted()){
                Event event = null;
                try {
                    event = eventQueue.take();
                } catch (InterruptedException e) {
                    if(!isStopped){
                        log.warn("AsyncDispatcher event handler thread is interrupted: " + e);
                    }
                    return;
                }
                if(event != null){
                    dispatch(event);
                }
                else{
                    throw new IllegalStateException("this dispatcher doesn't have eventhanler to handle event " + event);
                }
            }
        }

        public void shutdown(){
            isStopped = true;
        }
    }

    /**
     * 内置线程创建工厂,主要改变线程本身的一些属性
     */
    private final class InnerThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread now = new Thread(r);
            now.setName("AsyncDispatcher event handler-" + (pool.getCorePoolSize() - 1));
            return now;
        }
    }

    /**
     * 主要用于接受事件并放入事件队列,等待分发线程分派该事件
     * 支持动态改变线程数来满足事件及时分派的场景
     */
    class GenericEventHandler implements EventHandler<Event>{

        @Override
        public void handle(Event event) {
            try {
                //用于统计并打印相关信息日志
                int size = eventQueue.size();
                if(size > 0 && size % 1000 == 0){
                    log.info("event-queue size = " + size);
                }
                int remCapacity = eventQueue.remainingCapacity();
                if(remCapacity < 1000){
                    log.warn("Very low remaining capacity int the event-queue: " + remCapacity);
                }
                //end

                //如果event-queue大小过大,多开线程进行处理
                if(size > 0){
                    int coreSize = pool.getCorePoolSize();
                    //理想是每条线程处理1000个事件
                    int expectedSize = size % 1000 == 0? (size / 1000) : (size / 1000 + 1);
                    int finalSize = coreSize;
                    finalSize = expectedSize <= THREADS_LIMIT? expectedSize : THREADS_LIMIT;
                    pool.setCorePoolSize(finalSize);
                    if(finalSize >= coreSize){
                        runNEventHandlerThread(finalSize - coreSize);
                    }
                    else{
                        shutdownNEventHandlerThread(coreSize - finalSize);
                    }
                }
                //end

                eventQueue.put(event);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 一事件对应多个事件处理器的场景
     */
    class MultiListenerHandler implements EventHandler<Event>{
        List<EventHandler<Event>> handlers;

        public MultiListenerHandler() {
            this.handlers = new LinkedList<>();
        }

        @Override
        public void handle(Event event) {
            for(EventHandler<Event> handler: handlers){
                handler.handle(event);
            }
        }

        void addHandler(EventHandler<Event> handler){
            handlers.add(handler);
        }
    }
}
