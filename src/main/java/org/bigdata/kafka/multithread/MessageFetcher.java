package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Created by hjq on 2017/6/19.
 * Fetcher
 * 负责抓取信息的线程
 */
public class MessageFetcher<K, V> implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MessageFetcher.class);
    private KafkaConsumer<K, V> consumer;
    //等待提交的Offset
    //用Map的原因是如果同一时间内队列中有相同的topic分区的offset需要提交，那么map会覆盖原有的
    //使用ConcurrentSkipListMap保证key有序,key为TopicPartitionWithTime,其实现是以加入队列的时间来排序
    private ConcurrentHashMap<TopicPartition, OffsetAndMetadata> pendingOffsets = new ConcurrentHashMap();
    //线程状态标识
    private boolean isTerminated = false;
    //结束循环fetch操作
    private boolean isStopped = false;
    //重试相关属性
    private boolean enableRetry = false;
    private int maxRetry = 5;
    private int nowRetry = 0;
    //consumer poll timeout
    private long pollTimeout = 1000;
    //定时扫描注册中心并在发现新配置时及时更新运行环境
//    private ConfigFetcher configFetcher;
    private MessageHandlersManager handlersManager;

    private String assignDesc;

    public MessageFetcher(Properties properties) {
        this.consumer = new KafkaConsumer<K, V>(properties);

        //messagehandler.mode => OPOT/OPMT
        String mode = properties.get("messagehandler.mode").toString();
        if(mode.toUpperCase().equals("OPMT")){
            this.handlersManager = new OPMTMessageHandlersManager(10);
        }
        else{
            this.handlersManager = new OPOTMessageHandlersManager();
        }
    }

    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener){
        this.consumer.subscribe(topics, listener);
    }

    public void subscribe(Collection<String> topics){
        this.consumer.subscribe(topics);
    }

    public void registerHandlers(Map<String, Class<? extends MessageHandler>> topic2HandlerClass){
        handlersManager.registerHandlers(topic2HandlerClass);
    }

    public void registerCommitStrategies(Map<String, Class<? extends CommitStrategy>> topic2CommitStrategyClass){
        handlersManager.registerCommitStrategies(topic2CommitStrategyClass);
    }

    public void close(){
        log.info("consumer[" + assignment() + "] fetcher thread closing...");
        this.isStopped = true;
    }

    public boolean isTerminated() {
        return isTerminated;
    }

    public String assignment(){
//        if(assignDesc == null || assignDesc.equals("")){
//            assignDesc = StrUtil.topicPartitionsStr(consumer.assignment());
//        }
        return "";
    }

    public Map<TopicPartition, OffsetAndMetadata> getPendingOffsets() {
        return pendingOffsets;
    }

    @Override
    public void run() {
        long offset = -1;
        log.info("consumer[" + assignment() + "] fetcher thread started");
        try{
            //jvm缓存,当消息处理线程还没启动完,或者配置更改时,需先缓存消息,等待线程启动好再处理
            Queue<ConsumerRecord<K, V>> msgCache = new LinkedList<>();

            while(!isStopped && !Thread.currentThread().isInterrupted()){
                //查看有没offset需要提交
                Map<TopicPartition, OffsetAndMetadata> offsets = allPendingOffsets();

                if(offsets != null){
                    //有offset需要提交
                    log.info("consumer[" + assignment() + "] commit [" + offsets.size() + "] topic partition offsets");
                    commitOffsetsSync(offsets);
                }

                ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                for(TopicPartition topicPartition: records.partitions())
                    for(ConsumerRecord<K, V> record: records.records(topicPartition)){
                        if(record.offset() > offset){
                            offset = record.offset();
                        }
                        //按照某种策略提交线程处理
                        if(!handlersManager.dispatch(new ConsumerRecordInfo(record), pendingOffsets)){
                            log.info("OPOTMessageHandlersManager reconfig...");
                            log.info("message " + record.toString() + " add cache");
                            msgCache.add(record);
                        }
                    }

//                if(!handlersManager.isReConfig()){
//                    Iterator<ConsumerRecord<K, V>> iterator = msgCache.iterator();
//                    log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] handle cached message");
//                    while(iterator.hasNext()){
//                        ConsumerRecord<K, V> record = iterator.next();
//                        if(handlersManager.dispatch(new ConsumerRecordInfo(record, System.currentTimeMillis()), pendingOffsets)){
//                            //删除该元素
//                            iterator.remove();
//                        }
//                        else{
//                            //可能MessageHandlersManager又发生重新配置了,不处理后面的缓存
//                            log.info("OPOTMessageHandlersManager reconfig when handle cached message[bad]");
//                            break;
//                        }
//                    }
//                }

            }
        }
        finally {
            //等待所有该消费者接受的消息处理完成
            Set<TopicPartition> topicPartitions = this.consumer.assignment();
            handlersManager.consumerCloseNotify(topicPartitions);
            //关闭前,提交所有offset
            Map<TopicPartition, OffsetAndMetadata> topicPartition2Offset = allPendingOffsets();
            //同步提交
            commitOffsetsSync(topicPartition2Offset);
            log.info("consumer[" + assignment() + "] kafka conusmer closing...");
            this.consumer.close();
            log.info("consumer[" + assignment() + "] kafka conusmer closed");
            //有异常抛出,需设置,不然手动调用close就设置好了.
            isStopped = true;
            //标识线程停止
            isTerminated = true;
            log.info("consumer[" + assignment() + "] message fetcher closed");
            System.out.println("消费者端接受最大Offset: " + offset);
        }
    }

    private void commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets){
        if(offsets == null || offsets.size() <= 0){
            return;
        }

        log.info("consumer[" + assignment() + "] commit offsets Sync...");
        consumer.commitSync(offsets);
        log.info("consumer[" +MessageFetcher.this.assignment() + "] offsets [" + StrUtil.topicPartitionOffsetsStr(offsets) + "] committed");
    }

    private void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets){
        if(offsets == null || offsets.size() <= 0){
            return;
        }

        log.info("consumer[" + assignment() + "] commit offsets ASync...");
        consumer.commitAsync(offsets, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                //Exception e --> The exception thrown during processing of the request, or null if the commit completed successfully
                if(e !=null){
                    log.info("consumer[" + assignment() + "] commit offsets " + nowRetry + " times failed!!!");
                    //失败
                    if(enableRetry){
                        //允许重试,再次重新提交offset
                        if(nowRetry < maxRetry){
                            log.info("consumer[" + assignment() + "] retry commit offsets");
                            commitOffsetsAsync(offsets);
                            nowRetry ++;
                        }
                        else{
                            log.error("consumer[" + assignment() + "] retry times greater than " + maxRetry + " times(MaxRetry times)");
                            close();
                        }
                    }
                    else{
                        //不允许,直接关闭该Fetcher
                        log.error("consumer[" + assignment() + "] disable retry commit offsets");
                        close();
                    }
                }
                else{
                    //成功,打日志
                    nowRetry = 0;
                    log.info("consumer[" + MessageFetcher.this.assignment() + "] offsets [" + StrUtil.topicPartitionOffsetsStr(offsets) + "] committed");
                }
            }
        });
    }

    private Map<TopicPartition, OffsetAndMetadata> allPendingOffsets(){
        if(this.pendingOffsets.size() > 0){
            //复制所有需要提交的offset
            synchronized (this.pendingOffsets){
                Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
                for(Map.Entry<TopicPartition, OffsetAndMetadata> entry: this.pendingOffsets.entrySet()){
                    result.put(entry.getKey(), entry.getValue());
                }
                this.pendingOffsets.clear();
                return result;
            }
        }
        return null;
    }

    public abstract class InnerRebalanceListener implements ConsumerRebalanceListener{
        protected KafkaConsumer<K, V> consumer = MessageFetcher.this.consumer;
    }

    public class InMemoryRebalanceListsener extends InnerRebalanceListener{
        //jvm内存缓存目前消费到的Offset
        private Map<TopicPartition, Long> topicPartition2Offset = new HashMap<>();

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            log.info("consumer origin assignment: " + StrUtil.topicPartitionsStr(collection));
            log.info("consumer rebalancing...");
            //保存在jvm内存中
            for(TopicPartition topicPartition: collection){
                topicPartition2Offset.put(topicPartition, consumer.position(topicPartition));
            }
            //分区负载均衡时,至null,负载均衡后重新生成对应字符串
            assignDesc = null;

            //还需清理已经交给handler线程
            handlersManager.consumerRebalanceNotify();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            for(TopicPartition topicPartition: collection){
                consumer.seek(topicPartition, topicPartition2Offset.get(topicPartition));
            }
            log.info("consumer reassigned");
            log.info("consumer new assignment: " + StrUtil.topicPartitionsStr(collection));
            //清理offset缓存
            topicPartition2Offset.clear();
        }
    }
}
