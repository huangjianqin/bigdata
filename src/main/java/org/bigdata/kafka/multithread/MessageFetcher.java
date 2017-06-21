package org.bigdata.kafka.multithread;

import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;

/**
 * Created by hjq on 2017/6/19.
 */
public class MessageFetcher<K, V> implements Runnable {
    private KafkaConsumer<K, V> consumer;
    //等待提交的Offset
    //用Map的原因是如果同一时间内队列中有相同的topic分区的offset需要提交，那么map会覆盖原有的
    //使用ConcurrentSkipListMap保证key有序,key为TopicPartitionWithTime,其实现是以加入队列的时间来排序
    private Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets = new ConcurrentSkipListMap();
    //线程状态标识
    private boolean isTerminated = false;
    //结束循环fetch操作
    private boolean isStopped = false;
    //重试相关属性
    private boolean enableRetry = true;
    private int maxRetry = 5;
    private int nowRetry = 0;
    //consumer poll timeout
    private long pollTimeout = 1000;
    //定时扫描注册中心并在发现新配置时及时更新运行环境
    private ConfigFetcher configFetcher;

    public MessageFetcher(Properties properties) {
        this.consumer = new KafkaConsumer<K, V>(properties);
    }

    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener){
        this.consumer.subscribe(topics, listener);
    }

    public void subscribe(Collection<String> topics){
        this.consumer.subscribe(topics);
    }

    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener){
        this.consumer.subscribe(pattern, listener);
    }

    public void close(){
        this.isStopped = true;
    }

    public boolean isStopped() {
        return isStopped;
    }

    public boolean isTerminated() {
        return isTerminated;
    }

    public Map<TopicPartitionWithTime, OffsetAndMetadata> getPendingOffsets() {
        return pendingOffsets;
    }

    @Override
    public void run() {
        try{
            while(!isStopped && !Thread.currentThread().isInterrupted()){
                //查看有没offset需要提交
                Map<TopicPartition, OffsetAndMetadata> offsets = randomPendingOffsets();

                if(offsets != null){
                    //有offset需要提交
                    commitOffsetsAsync(offsets);
                }

                //jvm缓存,当消息处理线程还没启动完,或者配置更改时,需先缓存消息,等待线程启动好再处理
                Queue<ConsumerRecord<K, V>> msgCache = new LinkedList<>();

                ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                for(TopicPartition topicPartition: records.partitions())
                    for(ConsumerRecord<K, V> record: records.records(topicPartition)){
                        //按照某种策略提交线程处理
                        if(!MessageHandlersManager.instance().dispatch(new ConsumerRecordInfo(record, System.currentTimeMillis()), pendingOffsets)){
                            msgCache.add(record);
                        }
                    }

                if(!MessageHandlersManager.instance().isReConfig()){
                    Iterator<ConsumerRecord<K, V>> iterator = msgCache.iterator();
                    while(iterator.hasNext()){
                        ConsumerRecord<K, V> record = iterator.next();
                        if(MessageHandlersManager.instance().dispatch(new ConsumerRecordInfo(record, System.currentTimeMillis()), pendingOffsets)){
                            //删除该元素
                            iterator.remove();
                        }
                        else{
                            //可能MessageHandlersManager又发生重新配置了
                            break;
                        }
                    }
                }

            }
        }
        finally {
            //等待所有该消费者接受的消息处理完成
            Set<TopicPartition> topicPartitions = this.consumer.assignment();
            MessageHandlersManager.instance().consumerCloseNotify(topicPartitions);
            this.consumer.close();
            //有异常抛出,需设置,不然手动调用close就设置好了.
            isStopped = true;
            //标识线程停止
            isTerminated = true;
        }
    }

    private void commitOffsetsAsync(Map<TopicPartition, OffsetAndMetadata> offsets){
        consumer.commitAsync(offsets, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                //Exception e --> The exception thrown during processing of the request, or null if the commit completed successfully
                if(e !=null){
                    //失败
                    if(enableRetry){
                        //允许重试,再次重新提交offset
                        if(nowRetry < maxRetry){
                            for(Map.Entry<TopicPartition, OffsetAndMetadata> entry: map.entrySet()){
                                pendingOffsets.put(new TopicPartitionWithTime(entry.getKey(), System.currentTimeMillis()), entry.getValue());
                            }
                            nowRetry ++;
                        }
                    }
                    else{
                        //不允许,直接关闭该Fetcher
                        close();
                    }
                }
                else{
                    //成功,打日志
                }
            }
        });
    }

    private Map<TopicPartition, OffsetAndMetadata> randomPendingOffsets(){
        if(this.pendingOffsets.size() > 0){
            Map<TopicPartition, OffsetAndMetadata> result = new HashedMap();
            //只有一条线程读取该Map,不用给Map加锁
            //随机抽取m个offset提交
            int selectedOffsetSize = new Random(this.pendingOffsets.size()).nextInt();
            Iterator<Map.Entry<TopicPartitionWithTime, OffsetAndMetadata>> source = this.pendingOffsets.entrySet().iterator();
            while(source.hasNext() && selectedOffsetSize > 0){
                Map.Entry<TopicPartitionWithTime, OffsetAndMetadata> entry = source.next();
                result.put(entry.getKey().getTopicPartition(), entry.getValue());
                selectedOffsetSize --;
                source.remove();
            }
            return result;
        }
        return null;
    }

}
