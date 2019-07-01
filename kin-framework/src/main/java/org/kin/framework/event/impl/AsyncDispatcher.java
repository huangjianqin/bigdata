package org.kin.framework.event.impl;

import org.kin.framework.concurrent.SimpleThreadFactory;
import org.kin.framework.concurrent.ThreadManager;
import org.kin.framework.event.Dispatcher;
import org.kin.framework.event.Event;
import org.kin.framework.event.EventHandler;
import org.kin.framework.service.AbstractService;
import org.kin.framework.utils.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by 健勤 on 2017/8/8.
 * 异步事件分发器
 * 支持多线程事件处理
 */
public class AsyncDispatcher extends AbstractService implements Dispatcher {
    private static Logger log = LoggerFactory.getLogger("event");
    private static final int TOO_MUCH_EVENTS_THRESHOLD = 100;

    //缓存所有待分发的时间
    private final BlockingQueue<Event> eventQueue;
    //负责分发事件的线程
    private final ThreadManager threadManager;
    //存储事件与其对应的事件处理器的映射
    protected final Map<Class<? extends Enum>, EventHandler> event2Dispatcher;

    //负责将事件进队的事件处理器
    private final EventHandler innerHandler = new GenericEventHandler();
    //事件分发线程
    private List<EventHandlerThread> eventHandlerThreads = new LinkedList<>();
    //事件分发线程数最大值
    private final int THREADS_LIMIT;
    //是否开启优化，默认开启
    //积压待处理事件过多会开启更多线程进行处理
    private boolean optimized = false;
    //lock
    private final Object lock = new Object();


    public AsyncDispatcher() {
        this(new LinkedBlockingQueue<>(), Integer.MAX_VALUE, true);
    }

    public AsyncDispatcher(boolean optimized) {
        this(new LinkedBlockingQueue<Event>(), Integer.MAX_VALUE, optimized);
    }

    public AsyncDispatcher(int maxThreads) {
        this(new LinkedBlockingQueue<>(), maxThreads, true);
    }

    public AsyncDispatcher(int maxThreads, boolean optimized) {
        this(new LinkedBlockingQueue<>(), maxThreads, optimized);
    }

    public AsyncDispatcher(BlockingQueue<Event> eventQueue, int maxThreads, boolean optimized) {
        super("AsyncDispatcher");
        this.eventQueue = eventQueue;
        event2Dispatcher = new HashMap<>();
        this.optimized = optimized;
        this.THREADS_LIMIT = maxThreads;

        if (optimized && THREADS_LIMIT <= 1) {
            throw new IllegalStateException("开启优化, 线程数必须大于1");
        }

        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                1,
                this.THREADS_LIMIT,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new SimpleThreadFactory("AsyncDispatcher$event-handler-"));
        threadManager = new ThreadManager(pool);
    }

    //------------------------------------------------------------------------------------------------------------------

    /**
     * 通过该方法获得eventhandler并handle event,本质上是添加进队列
     */
    @Override
    public EventHandler getEventHandler() {
        return innerHandler;
    }

    protected static boolean checkEventClass(Class<? extends Enum> eventType, Type eventClass) {
        while (eventClass != null) {
            if (eventClass instanceof Class) {
                for (Type interfaceType : ((Class) eventClass).getGenericInterfaces()) {
                    if (interfaceType instanceof ParameterizedType) {
                        Type[] types = ((ParameterizedType) interfaceType).getActualTypeArguments();
                        for (Type type : types) {
                            if (type instanceof Class) {
                                return eventType.equals(type);
                            }
                        }
                    }
                }
                eventClass = ((Class) eventClass).getGenericSuperclass();
            } else if (eventClass instanceof ParameterizedType) {
                for (Type type : ((ParameterizedType) eventClass).getActualTypeArguments()) {
                    if (type instanceof Class) {
                        return eventType.equals(type);
                    }
                }
                eventClass = ((Class) ((ParameterizedType) eventClass).getRawType()).getGenericSuperclass();
            }
        }

        return false;
    }

    /**
     * 检查处理器处理的事件类型与@param eventType是否一致
     */
    private static boolean check(Class<? extends Enum> eventType, EventHandler handler) {
        if(handler instanceof SpringAsyncDispatcher.MethodAnnotationEventHandler){
            //无法从MethodAnnotationEventHandler获取到eventType类型
            SpringAsyncDispatcher.MethodAnnotationEventHandler methodAnnotationEventHandler = (SpringAsyncDispatcher.MethodAnnotationEventHandler) handler;
            return checkEventClass(eventType, methodAnnotationEventHandler.getEventClass());
        }
        else{
            //要实例化后才能获取
            Type eventHandlerInterfaceType = null;
            for(Type type: handler.getClass().getGenericInterfaces()){
                if(type instanceof ParameterizedType && ((ParameterizedType)type).getRawType().equals(EventHandler.class)){
                    eventHandlerInterfaceType = type;
                    break;
                }
                if(type instanceof Class && type.equals(EventHandler.class)){
                    eventHandlerInterfaceType = type;
                    break;
                }
            }

            if(eventHandlerInterfaceType != null){
                return checkEventClass(eventType, ((ParameterizedType) eventHandlerInterfaceType).getActualTypeArguments()[0]);
            }
        }

        return false;
    }

    @Override
    public void register(Class<? extends Enum> eventType, EventHandler handler) {
        //运行时检查, 无法做到在编译器检查
        if (check(eventType, handler)) {
            //event2Dispatcher需同步,防止多写的情况
            synchronized (event2Dispatcher) {
                EventHandler<Event> registered = event2Dispatcher.get(eventType);
                if (registered == null) {
                    event2Dispatcher.put(eventType, handler);
                } else if (!(registered instanceof MultiListenerHandler)) {
                    MultiListenerHandler multiHandler = new MultiListenerHandler();
                    multiHandler.addHandler(registered);
                    multiHandler.addHandler(handler);
                    event2Dispatcher.put(eventType, multiHandler);
                } else {
                    ((MultiListenerHandler) registered).addHandler(handler);
                }
            }
        } else {
            throw new IllegalStateException("处理器事件类型不一致");
        }
    }


    @Override
    public void dispatch(Event event) {
        Class<? extends Enum> type = event.getType().getDeclaringClass();
        EventHandler handler = event2Dispatcher.get(type);
        if (handler != null) {
            handler.handle(event);
        } else {
            throw new IllegalStateException("doesn't have event handler to handle event " + type);
        }
    }


    @Override
    public void serviceInit() {
        super.serviceInit();
    }

    @Override
    public void serviceStart() {
        //默认启动一条线程处理
        runNEventHandlerThread(1);
        super.serviceStart();
    }

    @Override
    public void serviceStop() {
        shutdownNEventHandlerThread(eventHandlerThreads.size());
        //用shutdownNow是为了强制中断阻塞在BlockingQueue.take()的线程
        threadManager.shutdownNow();

        super.serviceStop();
    }

    EventHandlerThread newThread() {
        return new EventHandlerThread();
    }

    void runNEventHandlerThread(int size) {
        synchronized (lock) {
            for (int i = 0; i < size; i++) {
                EventHandlerThread thread = newThread();
                eventHandlerThreads.add(thread);
                threadManager.submit(thread);
            }
        }
    }

    void shutdownNEventHandlerThread(int size) {
        synchronized (lock) {
            for (int i = 0; i < size; i++) {
                eventHandlerThreads.remove(0).shutdown();
            }
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    /**
     * 事件处理线程,主要逻辑是从事件队列获得事件并分派出去
     */
    private final class EventHandlerThread implements Runnable {
        private boolean isStopped = false;
        private Thread bindThread;

        @Override
        public void run() {
            this.bindThread = Thread.currentThread();
            while (!isStopped && !Thread.currentThread().isInterrupted()) {
                Event event;
                try {
                    event = eventQueue.take();
                    if (event != null) {
                        dispatch(event);
                    }
                } catch (InterruptedException e) {
                    if (!isStopped) {
                        log.warn("AsyncDispatcher event handler thread is interrupted: " + e);
                        //向外层抛异常，由外层统一处理

                    }
                    return;
                } finally {
                    //统一异常处理
                    synchronized (lock) {
                        eventHandlerThreads.remove(this);
                    }
                }
            }
        }

        void shutdown() {
            isStopped = true;
            this.bindThread.interrupt();
        }
    }

    /**
     * 主要用于接受事件并放入事件队列,等待分发线程分派该事件
     * 支持动态改变线程数来满足事件及时分派的场景
     */
    private class GenericEventHandler implements EventHandler<Event> {

        @Override
        public void handle(Event event) {
            try {
                //用于统计并打印相关信息日志
                int size = eventQueue.size();
                if (size > 0) {
                    log.info("event-queue size = " + size);
                }
                int remCapacity = eventQueue.remainingCapacity();
                if (remCapacity > TOO_MUCH_EVENTS_THRESHOLD) {
                    log.warn("high remaining capacity in the event-queue: " + remCapacity);
                }
                //end

                if (optimized) {
                    //如果event-queue大小过大,多开线程进行处理
                    if (size > 0) {
                        int coreSize = eventHandlerThreads.size();
                        //理想是每条线程处理1000个事件
                        int expectedSize = size % 1000 == 0 ? (size / 1000) : (size / 1000 + 1);
                        int finalSize;
                        finalSize = expectedSize <= THREADS_LIMIT ? expectedSize : THREADS_LIMIT;
                        if (finalSize >= coreSize) {
                            runNEventHandlerThread(finalSize - coreSize);
                        } else {
                            shutdownNEventHandlerThread(coreSize - finalSize);
                        }
                    }
                    //end
                }

                eventQueue.put(event);
            } catch (InterruptedException e) {
                ExceptionUtils.log(e);
            }
        }
    }

    /**
     * 一事件对应多个事件处理器的场景
     */
    private class MultiListenerHandler implements EventHandler<Event> {
        private List<EventHandler<Event>> handlers;

        MultiListenerHandler() {
            this.handlers = new LinkedList<>();
        }

        @Override
        public void handle(Event event) {
            for (EventHandler<Event> handler : handlers) {
                try {
                    handler.handle(event);
                } catch (Exception e) {
                    ExceptionUtils.log(e);
                }
            }
        }

        void addHandler(EventHandler<Event> handler) {
            handlers.add(handler);
        }
    }
}
