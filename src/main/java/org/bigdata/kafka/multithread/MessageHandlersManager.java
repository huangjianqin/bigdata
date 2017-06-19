package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by hjq on 2017/6/19.
 */
public class MessageHandlersManager {
    private static MessageHandlersManager handlersManager;
    private Map<String, MessageHandler> topic2Handler = new ConcurrentHashMap<>();
    private Map<String, CommitStrategy> topic2CommitStrategy = new ConcurrentHashMap<>();
    private Map<TopicPartition, MessageHandlerThread> topicPartition2Thread = new ConcurrentHashMap<>();
    private ThreadPoolExecutor threads = new ThreadPoolExecutor(2, Runtime.getRuntime().availableProcessors() * 2 - 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    private volatile boolean isReConfig = false;

    public static MessageHandlersManager instance(){
        if(handlersManager == null){
            synchronized (handlersManager){
                if(handlersManager == null){
                    handlersManager = new MessageHandlersManager();
                }
            }
        }
        return handlersManager;
    }

    public void registerHandler(String topic, MessageHandler handler){
        topic2Handler.put(topic, handler);
    }

    public void registerCommitStrategy(String topic, CommitStrategy strategy){
        topic2CommitStrategy.put(topic, strategy);
    }

    public void removeHandler(String topic){
        topic2Handler.remove(topic);
    }

    public void removeCommitStrategy(String topic){
        topic2CommitStrategy.remove(topic);
    }

    public boolean isReConfig() {
        return isReConfig;
    }

    public boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets){
        TopicPartition topicPartition = consumerRecordInfo.topicPartition();

        if(isReConfig){
            return false;
        }

        if(!topicPartition2Thread.containsKey(topicPartition)){
            //已有该topic分区对应的线程启动
            //直接添加队列
            topicPartition2Thread.get(topicPartition).queue().add(consumerRecordInfo);
        }
        else{
            //没有该topic分区对应的线程'
            //先启动线程,再添加至队列
            MessageHandlerThread thread = newThread(pendingOffsets);
            topicPartition2Thread.put(topicPartition, thread);
            thread.queue.add(consumerRecordInfo);
        }

        return true;
    }

    public void consumerCloseNotify(Set<TopicPartition> topicPartitions){
        boolean isAllHandled = false;
        List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitions);
        while(isAllHandled){
            for(TopicPartition topicPartition: topicPartitionList){
                //因为consumer没有关闭,所以不会导致该consumer所属的消费者组rebalance,
                //所以不会有该consumer负责的分区消息进去TopicPartition对应的线程拥有的队列
                Queue<ConsumerRecordInfo> queue = topicPartition2Thread.get(topicPartition).queue;
                for(ConsumerRecordInfo consumerRecordInfo: queue){
                   //提高将要关闭的consumer所fetch的消息的优先级,尽快处理,尽快关闭consumer
                }
            }
        }

    }

    public MessageHandlerThread newThread(Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets){
        return new MessageHandlerThread(pendingOffsets);
    }

    public class MessageHandlerThread implements Runnable{
        private Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets;
        private Queue<ConsumerRecordInfo> queue = new LinkedBlockingQueue<>();
        private boolean isStooped = false;

        public MessageHandlerThread(Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets) {
            this.pendingOffsets = pendingOffsets;
        }

        public Queue<ConsumerRecordInfo> queue() {
            return queue;
        }

        public boolean isStooped() {
            return isStooped;
        }

        public void close(){
            this.isStooped = true;
        }

        @Override
        public void run() {
            while(!this.isStooped && !Thread.currentThread().isInterrupted()){
                ConsumerRecordInfo record = queue.poll();
                execute(record);
            }

            //线程关闭时,要及时清理队列中剩余的ConsumerRecord
            for(ConsumerRecordInfo record: queue){
                execute(record);
            }
        }

        private void execute(ConsumerRecordInfo record){
            try {
                execute(record);
                record.callBack(null);
            } catch (Exception e) {
                try {
                    record.callBack(e);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }

        private void doExecute(ConsumerRecordInfo record) throws Exception {
            TopicPartition topicPartition = record.topicPartition();
            MessageHandlersManager.this.topic2Handler.get(topicPartition).handle(record.record());
            if(MessageHandlersManager.this.topic2CommitStrategy.get(topicPartition).isToCommit(record.record())){
                pendingOffsets.put(new TopicPartitionWithTime(topicPartition, System.currentTimeMillis()), new OffsetAndMetadata(record.record().offset() + 1));
            }

        }
    }
}
