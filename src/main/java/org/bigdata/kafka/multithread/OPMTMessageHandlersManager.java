package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by hjq on 2017/7/4.
 * OPMT ==> one partition more thread
 * 貌似这样子的设计CommitStrategy无法使用
 */
public class OPMTMessageHandlersManager extends AbstractMessageHandlersManager {
    private static Logger log = LoggerFactory.getLogger(OPMTMessageHandlersManager.class);
    private Map<TopicPartition, ThreadPoolExecutor> topicPartition2Pools = new HashMap<>();
    private Map<TopicPartition, PendingWindow> topicPartition2PendingWindow = new HashMap<>();

    private List<ThreadPoolExecutor> poolCache = new ArrayList<>();

    @Override
    public boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets) {
        log.debug("dispatching message: " + StrUtil.consumerRecordDetail(consumerRecordInfo.record()));

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
                }
                else{
                    //消息处理线程池还没启动,则启动并绑定
                    log.info("no thread pool cache, new one");
                    pool = new ThreadPoolExecutor(2, Runtime.getRuntime().availableProcessors(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
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

        MessageHandler handler = topic2Handler.get(topicPartition.topic());
        if(handler == null){
            log.info("message handler not set, use default");
            //消息处理器还没创建,则创建默认
            handler = new DefaultMessageHandler();
            topic2Handler.put(topicPartition.topic(), handler);
        }

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

        cleanMsgHandlersAndCommitStrategies();
    }

    @Override
    public void consumerRebalanceNotify() {
        isRebalance.set(true);

        log.info("clean up tasks");
        //清空线程池任务,缓存线程池,供下次使用
        for(ThreadPoolExecutor pool: topicPartition2Pools.values()){
            //先清空队列
            pool.getQueue().clear();
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
        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            //提交已完成处理的消息的最大offset
            pendingWindow.commitLatest(false);
            pendingWindow.clean();
        }

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
}
