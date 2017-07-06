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
            log.debug("dispatch failure ~~~ rebalancing...");
            return false;
        }

        TopicPartition topicPartition = consumerRecordInfo.topicPartition();

        ThreadPoolExecutor pool = topicPartition2Pools.get(topicPartition);
        if(pool == null){
            synchronized (poolCache){
                if(poolCache.size() > 0){
                    //有线程池缓存
                    pool = poolCache.remove(0);
                }
                else{
                    //消息处理线程池还没启动,则启动并绑定
                    pool = new ThreadPoolExecutor(2, Runtime.getRuntime().availableProcessors(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
                }
            }

            topicPartition2Pools.put(topicPartition, pool);
        }

        PendingWindow pendingWindow = topicPartition2PendingWindow.get(topicPartition);
        if(pendingWindow == null){
            //等待offset连续完整窗口还没创建,则新创建
            pendingWindow = new PendingWindow(30, pendingOffsets);
            topicPartition2PendingWindow.put(topicPartition, pendingWindow);
        }

        MessageHandler handler = topic2Handler.get(topicPartition.topic());
        if(handler == null){
            //消息处理器还没创建,则创建默认
            handler = new DefaultMessageHandler();
            topic2Handler.put(topicPartition.topic(), handler);
        }

        pool.submit(new MessageHandlerTask(handler, pendingWindow, consumerRecordInfo));

        return true;
    }

    @Override
    public void consumerCloseNotify(Set<TopicPartition> topicPartitions) {
        //提交已完成处理的消息的最大offset
        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            pendingWindow.commitLatest();
        }

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

        //清空线程池任务,缓存线程池,供下次使用
        for(ThreadPoolExecutor pool: topicPartition2Pools.values()){
            //先清空队列
            pool.getQueue().clear();
        }

        //等待正在处理的线程处理完
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
            for(ThreadPoolExecutor pool: pools){
                pool.shutdownNow();
            }
        }

        //PendingWindow实例不实施缓存
        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            //提交已完成处理的消息的最大offset
            pendingWindow.commitLatest();
            pendingWindow.clean();
        }

        //启动缓存清理线程,当缓存2分钟内没有命中,则清楚已有的线程池缓存
        startExpireCleanPoolCache();
    }

    private void startExpireCleanPoolCache(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().sleep(1000 * 60 * 2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                synchronized (poolCache){
                    poolCache.clear();
                }
            }
        }).start();
    }

    private class MessageHandlerTask implements Runnable{
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
                handler.handle(target.record());
                pendingWindow.commitFinished(target);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
