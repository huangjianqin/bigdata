package org.kin.framework.concurrent;

import org.kin.framework.concurrent.impl.HashPartitioner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by huangjianqin on 2017/10/26.
 * 利用Task的某种属性将task分区,从而达到统一类的task按submit/execute顺序在同一线程执行
 */
public class PartitionTaskExecutors<K> {
    //当前分区数
    private int numPartition;
    //最大分区数
    private int maxNumPartition;

    //分区算法
    private Partitioner<K> partitioner;
    //执行线程池
    private ThreadPoolExecutor threadPool;
    //所有分区执行线程实例
    private List<PartitionTask> partitionTasks;

    public PartitionTaskExecutors(int numPartition) {
        this(numPartition, Integer.MAX_VALUE, new HashPartitioner<K>());
    }

    public PartitionTaskExecutors(int numPartition, int maxNumPartition) {
        this(numPartition, maxNumPartition, new HashPartitioner<K>());
    }

    public PartitionTaskExecutors(int numPartition, int maxNumPartition, Partitioner<K> partitioner) {
        this.numPartition = numPartition;
        this.maxNumPartition = maxNumPartition;
        this.threadPool = new ThreadPoolExecutor(numPartition, maxNumPartition, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        this.threadPool.allowCoreThreadTimeOut(true);
        this.partitioner = partitioner;
        this.partitionTasks = new ArrayList<>();
    }

    public void init(){
        startTask(numPartition);
    }

    public Future<?> execute(K key, Runnable task){
        FutureTask futureTask = new FutureTask(task, null);
        partitionTasks.get(partitioner.toPartition(key, numPartition)).execute(new Task(key, futureTask));
        return futureTask;
    }

    public <T> Future<T> execute(K key, Callable<T> task){
        FutureTask<T> futureTask = new FutureTask(task);
        partitionTasks.get(partitioner.toPartition(key, numPartition)).execute(new Task(key, futureTask));
        return futureTask;
    }

    private void shutdownTask(int num){
        List<PartitionTask> removedPartitionTasks = new ArrayList<>();
        //关闭并移除分区执行线程实例,且缓存
        for(int i = 0; i < num; i++){
            PartitionTask task = partitionTasks.remove(0);
            task.close();
            removedPartitionTasks.add(task);
        }

        //Executors doesn't shutdown
        if(partitionTasks.size() > 0){
            for(PartitionTask partitionTask: removedPartitionTasks){
                while (!partitionTask.isTerminated){
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                //重新执行task
                for(Task queuedTask: partitionTask.queue) {
                    execute(queuedTask.key, queuedTask.target);
                }

            }
        }
    }

    public void shutdown(){
        //先关闭执行线程实例再关闭线程池
        shutdownTask(partitionTasks.size());
        threadPool.shutdown();
    }

    public void shutdownNow(){
        threadPool.shutdownNow();
    }

    private void startTask(int num){
        for(int i = 0; i < num; i++){
            PartitionTask task = new PartitionTask();
            //先提交到线程池执行,再缓存
            threadPool.submit(task);
            partitionTasks.add(task);
        }
    }

    public void expandTo(int newPartitionNum){
        if(newPartitionNum > maxNumPartition) {
            throw new IllegalStateException(String.format("param newPartitionNum '%s' is greater than maxPartition '%s'", newPartitionNum, maxNumPartition));
        }
        startTask(newPartitionNum - numPartition);
        numPartition = newPartitionNum;
        threadPool.setCorePoolSize(numPartition);
    }

    public void expand(int addPartitionNum){
        int newPartitionNum = numPartition + addPartitionNum;
        expandTo(newPartitionNum);
    }

    public void shrink(int reducePartitionNum){
        int newPartitionNum = numPartition - reducePartitionNum;
        shrinkTo(newPartitionNum);
    }

    public void shrinkTo(int newPartitionNum){
        if(newPartitionNum < 0) {
            throw new IllegalStateException(String.format("param newPartitionNum '%s' can't be zero or negative", newPartitionNum));
        }
        //先分区较少,防止IndexOutOfBound
        int originNumPartition = numPartition;
        numPartition = newPartitionNum;

        shutdownTask(originNumPartition - newPartitionNum);
        threadPool.setCorePoolSize(originNumPartition);
    }

    private class Task implements Runnable{
        //缓存分区key,以便重分区时获取分区key
        private final K key;
        private final Runnable target;

        Task(K key, Runnable target) {
            this.key = key;
            this.target = target;
        }

        @Override
        public void run() {
            target.run();
        }
    }

    /**
     * task 执行
     */
    private class PartitionTask implements Runnable{
        //任务队列
        private BlockingQueue<Task> queue;
        //绑定的线程
        private Thread bind;

        private boolean isStopped = false;
        private boolean isTerminated = false;

        public PartitionTask() {
            queue = new LinkedBlockingQueue<Task>();
        }

        public void execute(Task task) {
            try {
                queue.put(task);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void close(){
            isStopped = true;
            if(bind != null){
                bind.interrupt();
            }
        }

        public void run() {
            bind = Thread.currentThread();
            while(!isStopped && !Thread.currentThread().isInterrupted()){
                try {
                    Task task = queue.take();
                    task.run();
                } catch (InterruptedException e) {

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            isTerminated = true;
        }
    }
}
