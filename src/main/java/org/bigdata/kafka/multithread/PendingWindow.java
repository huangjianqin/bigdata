package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;


/**
 * Created by hjq on 2017/7/4.
 */
public class PendingWindow {
    private static Logger log = LoggerFactory.getLogger(PendingWindow.class);
    private Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets;
    private PriorityBlockingQueue<ConsumerRecordInfo> queue;
    private final int slidingWindow;

    public PendingWindow(int slidingWindow, Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets) {
        this.slidingWindow = slidingWindow;
        this.pendingOffsets = pendingOffsets;
        this.queue = new PriorityBlockingQueue<>(slidingWindow, new Comparator<ConsumerRecordInfo>(){

            @Override
            public int compare(ConsumerRecordInfo o1, ConsumerRecordInfo o2) {
                if(o2 == null){
                    return 1;
                }

                long offset1 = o1.record().offset();
                long offset2 = o2.record().offset();

                if(offset1 > offset2){
                    return 1;
                }
                else if(offset1 == offset2){
                    return 0;
                }
                else {
                    return -1;
                }
            }
        });
    }

    public synchronized void clean(){
        queue.clear();
    }

    public void commitFinished(ConsumerRecordInfo record){
        queue.put(record);

        OffsetAndMetadata offset = getPendingOffset();

        if(offset != null){
            String topic = record.record().topic();
            int partition = record.record().partition();
            pendingToCommit(new TopicPartitionWithTime(new TopicPartition(topic, partition), System.currentTimeMillis()), offset);
        }
    }

    public void commitLatest(){
        long maxOffset;
        String topic;
        int partition;
        synchronized (queue){
            if(queue.size() <= 0){
                return;
            }

            ConsumerRecordInfo[] tmp = new ConsumerRecordInfo[queue.size()];
            queue.toArray(tmp);

            maxOffset = tmp[0].record().offset();
            topic = tmp[0].record().topic();
            partition = tmp[0].record().partition();
            for(int i = 1; i < queue.size(); i++){
                long thisOffset = tmp[i].record().offset();
                //判断offset是否连续
                if(thisOffset - maxOffset == 1){
                    maxOffset = thisOffset;
                }
                else{
                    break;
                }
            }
        }
        pendingToCommit(new TopicPartitionWithTime(new TopicPartition(topic, partition), System.currentTimeMillis()), new OffsetAndMetadata(maxOffset + 1));
    }

    private synchronized OffsetAndMetadata getPendingOffset(){
        //队列长度小于窗口大小,则是不需要提交Offset
        if(queue.size() < slidingWindow){
            return null;
        }

        ConsumerRecordInfo[] tmp = new ConsumerRecordInfo[queue.size()];
        queue.toArray(tmp);

        long maxOffset = tmp[0].record().offset();
        for(int i = 1; i < slidingWindow; i++){
            long thisOffset = tmp[i].record().offset();
            //判断offset是否连续
            if(thisOffset - maxOffset == 1){
                maxOffset = thisOffset;
            }
            else{
                return null;
            }
        }

        //移除前slidingWindow个已处理消息
        for(int i = 1; i < slidingWindow; i++){
            queue.poll();
        }

        return new OffsetAndMetadata(maxOffset + 1);
    }

    private void pendingToCommit(TopicPartitionWithTime topicPartitionWithTime, OffsetAndMetadata offset){
        this.pendingOffsets.put(topicPartitionWithTime, offset);
    }

}
