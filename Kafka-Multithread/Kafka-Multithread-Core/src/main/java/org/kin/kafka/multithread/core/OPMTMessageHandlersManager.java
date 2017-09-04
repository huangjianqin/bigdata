package org.kin.kafka.multithread.core;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.api.MessageHandler;
import org.kin.kafka.multithread.util.ConsumerRecordInfo;
import org.kin.kafka.multithread.util.StrUtil;
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
 *  1.当高负载的时候,会存在poll()时间执行过长而导致session timeout的可能
 *  这可能是机器CPU资源不够以无法在给定时间内执行相关操作,也有可能就是封装得不够好
 *  可以延长session超时时间或者调整CommitStrategy,提高提交Offset的频率
 *
 *  还是使用OPOT版本,可承受高负载,多开几个实例就好了.
 *
 *  2.该模式下,不能保证MessageHandler线程安全
 */
public class OPMTMessageHandlersManager extends AbstractMessageHandlersManager {
    private static final Logger log = LoggerFactory.getLogger(OPMTMessageHandlersManager.class);
    private Map<TopicPartition, ThreadPoolExecutor> topicPartition2Pools = new HashMap<>();
    private Map<TopicPartition, PendingWindow> topicPartition2PendingWindow = new HashMap<>();
    private Map<TopicPartition, List<MessageHandler>> topicPartition2MessageHandlers = new HashMap<>();
    private Map<TopicPartition, Long> topicPartition2Counter = new HashMap<>();

    private final int handlerSize;
    private final int threadSizePerPartition;
    private final int threadQueueSizePerPartition;

    public OPMTMessageHandlersManager(int handlerSize, int threadSizePerPartition, int threadQueueSizePerPartition) {
        this.handlerSize = handlerSize;
        this.threadSizePerPartition = threadSizePerPartition;
        this.threadQueueSizePerPartition = threadQueueSizePerPartition;
    }

    @Override
    public boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartition, OffsetAndMetadata> pendingOffsets) {
        log.debug("dispatching message: " + StrUtil.consumerRecordDetail(consumerRecordInfo.record()));

        if(isRebalance.get()){
            log.debug("dispatch failure due to rebalancing...");
            return false;
        }

        TopicPartition topicPartition = consumerRecordInfo.topicPartition();

        ThreadPoolExecutor pool = topicPartition2Pools.get(topicPartition);
        if(pool == null){
            //消息处理线程池还没启动,则启动并绑定
            log.info("no thread pool cache, new one(MaxPoolSize = " + threadSizePerPartition + ", QueueSize = "  + threadQueueSizePerPartition + ")");
            pool = new ThreadPoolExecutor(2, threadSizePerPartition, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(this.threadQueueSizePerPartition));
            topicPartition2Pools.put(topicPartition, pool);
        }

        PendingWindow pendingWindow = topicPartition2PendingWindow.get(topicPartition);
        if(pendingWindow == null){
            log.info("new pending window");
            //等待offset连续完整窗口还没创建,则新创建
            pendingWindow = new PendingWindow(1000, pendingOffsets);
            topicPartition2PendingWindow.put(topicPartition, pendingWindow);
        }

        List<MessageHandler> messageHandlers = topicPartition2MessageHandlers.get(topicPartition);
        if(messageHandlers == null){
            //round选择message handler
            log.info("init message handlers(size = " + handlerSize + ")");
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
    public void consumerCloseNotify() {
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

    }

    @Override
    public void consumerRebalanceNotify(Set<TopicPartition> topicPartitions) {
        isRebalance.set(true);

        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            //提交已完成处理的消息的最大offset
            pendingWindow.commitLatest(true);
        }
    }

    @Override
    public void doOnConsumerReAssigned(Set<TopicPartition> topicPartitions) {
        log.info("clean up target tasks");
        List<ThreadPoolExecutor> pools = new ArrayList<>();
        //清空线程池任务
        for(TopicPartition topicPartition: topicPartitions){
            //先清空队列
            topicPartition2Pools.get(topicPartition).getQueue().clear();
            pools.add(topicPartition2Pools.get(topicPartition));
        }

        if(pools.size() > 0){
            //等待正在处理的线程处理完
            log.info("waiting active task to finish and choose to cache thread pools...");
            int count = 0;
            while(count < 5){
                boolean isActive = false;

                for(ThreadPoolExecutor pool: pools){
                    if(pool.getActiveCount() > 0){
                        isActive = true;
                    }
                }

                if(isActive){
                    count ++;
                    try {
                        Thread.currentThread().sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                else{
                    break;
                }
            }

            //超过10s,强制关闭仍然在执行的现有资源
            if(count >= 5){
                log.info("waitting time out, force shutdown target active thread pool");
                for(ThreadPoolExecutor pool: pools){
                    pool.shutdownNow();
                }
            }
        }

        log.info("clean up target used pending window");
        for(TopicPartition topicPartition: topicPartitions){
            //提交已完成处理的消息的最大offset
            topicPartition2PendingWindow.get(topicPartition).commitLatest(true);
            topicPartition2PendingWindow.remove(topicPartition);
        }

        isRebalance.set(false);
    }

    private final class MessageHandlerTask implements Runnable{
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

}
