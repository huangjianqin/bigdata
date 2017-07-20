package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by hjq on 2017/7/4.
 * OPMT ==> one partition more thread
 * 貌似这样子的设计CommitStrategy无法使用
 *
 * 似乎这种方法是通用实现,只要把线程池线程数设置为1,那么就单线程版本咯
 * 虽然是通用版本,但是大量的线程切换导致性能开销
 *
 * 潜在问题:
 *  当高负载的时候,会存在poll()时间执行过长而导致session timeout的可能
 *  这可能是机器CPU资源不够以无法在给定时间内执行相关操作,也有可能就是封装得不够好
 *  还是使用OPOT版本,可承受高负载,多开几个实例就好了.
 *  
 */
public class OPMTMessageHandlersManager extends AbstractMessageHandlersManager {
    private static Logger log = LoggerFactory.getLogger(OPMTMessageHandlersManager.class);
    private Map<TopicPartition, ThreadPoolExecutor> topicPartition2Pools = new HashMap<>();
    private Map<TopicPartition, PendingWindow> topicPartition2PendingWindow = new HashMap<>();
    private Map<TopicPartition, List<MessageHandler>> topicPartition2MessageHandlers = new HashMap<>();
    private Map<TopicPartition, Long> topicPartition2Counter = new HashMap<>();
    //Rebalance时缓存已经拥有的线程池
    private List<ThreadPoolExecutor> poolCache = new ArrayList<>();
//    //负责重新提交被拒绝的任务线程
//    private TaskReSubmitThread taskReSubmitThread;
    private final int handlerSize;

    public OPMTMessageHandlersManager(int handlerSize) {
        this.handlerSize = handlerSize;
    }

    @Override
    public boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartition, OffsetAndMetadata> pendingOffsets) {
        log.debug("dispatching message: " + StrUtil.consumerRecordDetail(consumerRecordInfo.record()));

//        //初始化负责重新提交被拒绝的任务线程
//        if(taskReSubmitThread == null){
//            taskReSubmitThread = new TaskReSubmitThread();
//            new Thread(taskReSubmitThread).start();
//        }

        if(isRebalance.get()){
            log.debug("dispatch failure due to rebalancing...");
            return false;
        }

        TopicPartition topicPartition = consumerRecordInfo.topicPartition();

        ThreadPoolExecutor pool = topicPartition2Pools.get(topicPartition);
        if(pool == null){
            synchronized (poolCache){
                if(poolCache.size() > 0){
                    //有线程池缓存
                    log.info("hit thread pool cache, take one");
                    pool = poolCache.remove(0);
//                    ReSubmitRejectedTaskHandler handler = (ReSubmitRejectedTaskHandler)pool.getRejectedExecutionHandler();
//                    handler.setQueue(taskReSubmitThread.newQueue(topicPartition));
//                    taskReSubmitThread.setThreadPool(topicPartition, pool);
                }
                else{
                    //消息处理线程池还没启动,则启动并绑定
                    log.info("no thread pool cache, new one");
//                    ReSubmitRejectedTaskHandler handler = new ReSubmitRejectedTaskHandler();
//                    handler.setQueue(taskReSubmitThread.newQueue(topicPartition));
//                    pool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors(), 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), handler);
//                    taskReSubmitThread.setThreadPool(topicPartition, pool);
                    pool = new ThreadPoolExecutor(1,1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
                }
            }

            topicPartition2Pools.put(topicPartition, pool);
        }

        PendingWindow pendingWindow = topicPartition2PendingWindow.get(topicPartition);
        if(pendingWindow == null){
            log.info("new pending window");
            //等待offset连续完整窗口还没创建,则新创建
            pendingWindow = new PendingWindow(100000, pendingOffsets);
            topicPartition2PendingWindow.put(topicPartition, pendingWindow);
        }

        List<MessageHandler> messageHandlers = topicPartition2MessageHandlers.get(topicPartition);
        if(messageHandlers == null){
            messageHandlers = new ArrayList<>();
            for(int i = 0; i < handlerSize; i++){
                messageHandlers.add(newMessageHandler(topicPartition.topic()));
            }
            topicPartition2Counter.put(topicPartition, 1L);
            topicPartition2MessageHandlers.put(topicPartition, messageHandlers);
        }
        //round选择message handler
        MessageHandler handler = messageHandlers.get((int)(topicPartition2Counter.get(topicPartition) % messageHandlers.size()));
        topicPartition2Counter.put(topicPartition, topicPartition2Counter.get(topicPartition) + 1);

        log.debug("message: " + StrUtil.consumerRecordDetail(consumerRecordInfo.record()) + " wrappered as task has submit");
        pool.submit(new MessageHandlerTask(handler, pendingWindow, consumerRecordInfo));

        return true;
    }

    @Override
    public void consumerCloseNotify(Set<TopicPartition> topicPartitions) {
        //提交已完成处理的消息的最大offset
        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            pendingWindow.commitLatest(false);
        }

        log.info("shutdown thread pools...");
        //关闭线程池
        for(ThreadPoolExecutor pool: topicPartition2Pools.values()){
            //先清空队列
            pool.getQueue().clear();
            //再关闭
            pool.shutdown();
        }

//        //关闭负责重新提交被拒绝任务线程
//        taskReSubmitThread.cleanUp();
//        taskReSubmitThread.shutdown();
    }

    @Override
    public void consumerRebalanceNotify() {
        isRebalance.set(true);

//        //清理负责重新提交被拒绝任务线程
//        taskReSubmitThread.cleanUp();

        log.info("clean up tasks");
        //清空线程池任务,缓存线程池,供下次使用
        for(ThreadPoolExecutor pool: topicPartition2Pools.values()){
            //先清空队列
            pool.getQueue().clear();
            log.info("set up ReSubmitRejectedTaskHandler to discard all rejected task");
//            //抛弃所有被拒绝的task
//            ReSubmitRejectedTaskHandler handler = (ReSubmitRejectedTaskHandler)pool.getRejectedExecutionHandler();
//            handler.setQueue(null);
        }

        //等待正在处理的线程处理完
        log.info("waiting active task to finish and choose to cache thread pools...");
        int count = 0;
        List<ThreadPoolExecutor> pools = new ArrayList<>(topicPartition2Pools.values());
        while(count < 5){
            boolean isActive = false;

            List<ThreadPoolExecutor> activePools = new ArrayList<>();
            for(ThreadPoolExecutor pool: pools){
                if(pool.getActiveCount() > 0){
                    isActive = true;
                    activePools.add(pool);
                }
                else{
                    //添加空闲线程池至缓存
                    poolCache.add(pool);
                }
            }
            //移除空闲线程池
            pools.removeAll(poolCache);

            if(isActive){
                count ++;
                try {
                    Thread.currentThread().sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //超过10s,强制关闭仍然在执行的现有资源
        if(count == 5){
            log.info("waitting time out, force shutdown all active thread pool");
            for(ThreadPoolExecutor pool: pools){
                pool.shutdownNow();
            }
        }
        else{
            log.info("all active task finished, cache all idle thread pools");
        }

        log.info("clean up all used pending window");
        //PendingWindow实例不实施缓存
        topicPartition2PendingWindow.clear();

        //启动缓存清理线程,当缓存2分钟内没有命中,则清楚已有的线程池缓存
        startExpireCleanPoolCache();
    }

    private void startExpireCleanPoolCache(){
        log.info("start daemon thread to clean up cached thread pools after 2 minutes");
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                synchronized (poolCache){
                    poolCache.clear();
                }
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, 1000 * 60 * 2);
    }

    private class MessageHandlerTask implements Runnable{
        private Logger log = LoggerFactory.getLogger(MessageHandlerTask.class);
        private MessageHandler handler;
        private PendingWindow pendingWindow;
        private ConsumerRecordInfo target;

        public MessageHandlerTask(MessageHandler handler, PendingWindow pendingWindow, ConsumerRecordInfo target) {
            this.handler = handler;
            this.pendingWindow = pendingWindow;
            this.target = target;
        }

        @Override
        public void run() {
            try {
                log.debug(Thread.currentThread().getName() + " start to handle task... [" + target + "]");
                handler.handle(target.record());
                pendingWindow.commitFinished(target);
                log.debug(Thread.currentThread().getName() + " has finished handling task ");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

//    /**
//     * 用于重新提交线程池拒绝执行的线程
//     * 目的在弥补SynchronousQueue的缺陷,其是入队时马上执行task,否则拒绝.
//     * 处理消息是有空闲时候和峰值
//     * 为了更好利用机器资源,空闲时候保存一定量的线程,而峰值的时候性能全开,那么为了动态增加线程,必须使用SynchronousQueue,然而无法马上
//     * 转移task的话就会抛出异常,为了解决这个问题,SynchronousQueue转移task失败,而pool reject execution时,通过RejectedExecutionHandler
//     * 交给额外的线程进行重新提交.
//     * 此时,如果空闲时,线程池线程数就会是corePoolSize,峰值时,就会是MaxPoolSize
//     */
//    private class TaskReSubmitThread implements Runnable{
//        private Map<TopicPartition, Queue<Runnable>> topicPartition2RejectedTaskQueue = new HashMap<>();
//        private Map<TopicPartition, ExecutorService> topicPartition2ThreadPool = new HashMap<>();
//        private boolean isStopped = false;
//        private long duration = 5;
//
//        public TaskReSubmitThread() {
//        }
//
//        public TaskReSubmitThread(long duration) {
//            this.duration = duration;
//        }
//
//        public void setThreadPool(TopicPartition topicPartition, ExecutorService threadPool){
//            newQueue(topicPartition);
//            topicPartition2ThreadPool.put(topicPartition, threadPool);
//        }
//
//        public synchronized Queue<Runnable> newQueue(TopicPartition topicPartition){
//            //存在可能竞争,因为fetch线程插入,本线程读取
//            topicPartition2RejectedTaskQueue.putIfAbsent(topicPartition, new ConcurrentLinkedQueue<Runnable>());
//            return topicPartition2RejectedTaskQueue.get(topicPartition);
//        }
//
//        public Queue<Runnable> getQueue(TopicPartition topicPartition){
//            return topicPartition2RejectedTaskQueue.get(topicPartition);
//        }
//
//        public synchronized void cleanUp(){
//            log.info("clean up task resubmit thread");
//            topicPartition2RejectedTaskQueue.clear();
//            topicPartition2ThreadPool.clear();
//        }
//
//        public void shutdown(){
//            log.info("shutdown task resubmit thread");
//            isStopped = true;
//        }
//
//        @Override
//        public void run() {
//            log.info("task resubmit thread inited");
//            while(!isStopped && !Thread.currentThread().isInterrupted()){
//                //重新提交被拒绝任务
//                for(TopicPartition topicPartition: topicPartition2RejectedTaskQueue.keySet()){
//                    Queue<Runnable> queue = topicPartition2RejectedTaskQueue.get(topicPartition);
//                    ExecutorService threadPool = topicPartition2ThreadPool.get(topicPartition);
//
//                    if(queue.size() > 0){
//                        threadPool.submit(queue.poll());
//                    }
//                }
//
//                try {
//                    Thread.sleep(duration);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }
//
//    /**
//     * 延迟重新提交task
//     * 如果queue为空,则抛弃rejected task
//     */
//    private class ReSubmitRejectedTaskHandler implements RejectedExecutionHandler{
//        private Queue<Runnable> queue;
//
//        public void setQueue(Queue<Runnable> queue) {
//            this.queue = queue;
//        }
//
//        @Override
//        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
//            if(queue != null){
//                log.debug("resubmit rejected task");
//                queue.offer(r);
//            }
//        }
//    }

}
