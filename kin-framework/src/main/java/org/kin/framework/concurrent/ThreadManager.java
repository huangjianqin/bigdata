package org.kin.framework.concurrent;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.kin.framework.JvmCloseCleaner;
import org.kin.framework.utils.Constants;
import org.kin.framework.utils.StringUtils;
import org.kin.framework.utils.SysUtils;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by huangjianqin on 2018/1/24.
 */
public class ThreadManager implements ScheduledExecutorService {
    public static ThreadManager DEFAULT;
    public static final ExecutorType DEFAULT_EXECUTOR_TYPE;

    static {
        String executorTypeStr = System.getenv(Constants.DEFAULT_EXECUTOR);
        if (StringUtils.isNotBlank(executorTypeStr)) {
            DEFAULT_EXECUTOR_TYPE = ExecutorType.getByName(executorTypeStr);
            DEFAULT = new ThreadManager(DEFAULT_EXECUTOR_TYPE.getExecutor(), getDefaultScheduledExecutor());
        } else {
            DEFAULT_EXECUTOR_TYPE = ExecutorType.THREADPOOL;
            DEFAULT = new ThreadManager(DEFAULT_EXECUTOR_TYPE.getExecutor(), getDefaultScheduledExecutor());
        }

        JvmCloseCleaner.DEFAULT().add(() -> {
            DEFAULT.shutdown();
        });
    }

    //执行线程
    private ExecutorService executor;
    //调度线程
    private ScheduledExecutorService scheduleExecutor;

    private ThreadManager() {
    }

    public ThreadManager(ExecutorService executor) {
        this(executor, null);
    }

    public ThreadManager(ScheduledExecutorService scheduleExecutor) {
        this(null, scheduleExecutor);
    }

    public ThreadManager(ExecutorService executor, ScheduledExecutorService scheduleExecutor) {
        this.executor = executor;
        this.scheduleExecutor = scheduleExecutor;
    }

    public static ThreadManager forkJoinPoolThreadManager() {
        return new ThreadManager(ExecutorType.FORKJOIN.getExecutor());
    }

    public static ThreadManager forkJoinPoolThreadManagerWithScheduled() {
        return new ThreadManager(ExecutorType.FORKJOIN.getExecutor(), getDefaultScheduledExecutor());
    }

    public static ThreadManager commonThreadManager() {
        return new ThreadManager(ExecutorType.THREADPOOL.getExecutor());
    }

    public static ThreadManager commonThreadManagerWithScheduled() {
        return new ThreadManager(ExecutorType.THREADPOOL.getExecutor(), getDefaultScheduledExecutor());
    }

    //--------------------------------------------------------------------------------------------

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        Preconditions.checkNotNull(scheduleExecutor);
        return scheduleExecutor.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        Preconditions.checkNotNull(scheduleExecutor);
        return scheduleExecutor.schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        Preconditions.checkNotNull(scheduleExecutor);
        return scheduleExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        Preconditions.checkNotNull(scheduleExecutor);
        return scheduleExecutor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public void shutdown() {
        if(executor != null){
            executor.shutdown();
        }
        if(scheduleExecutor != null){
            scheduleExecutor.shutdown();
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> tasks = Lists.newArrayList();
        if(executor != null){
            tasks.addAll(executor.shutdownNow());
        }
        if(scheduleExecutor != null){
            tasks.addAll(scheduleExecutor.shutdownNow());
        }
        return tasks;
    }

    @Override
    public boolean isShutdown() {
        return (executor == null || executor.isShutdown()) && (scheduleExecutor == null || scheduleExecutor.isShutdown());
    }

    @Override
    public boolean isTerminated() {
        return (executor == null || executor.isTerminated()) && (scheduleExecutor == null || scheduleExecutor.isTerminated());
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = true;
        if(executor != null){
            result &= executor.awaitTermination(timeout, unit);
        }
        if(scheduleExecutor != null){
            result &= scheduleExecutor.awaitTermination(timeout, unit);
        }
        return result;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        Preconditions.checkNotNull(executor);
        return executor.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        Preconditions.checkNotNull(executor);
        return executor.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        Preconditions.checkNotNull(executor);
        return executor.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        Preconditions.checkNotNull(executor);
        return executor.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        Preconditions.checkNotNull(executor);
        return executor.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        Preconditions.checkNotNull(executor);
        return executor.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        Preconditions.checkNotNull(executor);
        return executor.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        Preconditions.checkNotNull(executor);
        executor.execute(command);
    }

    //--------------------------------------------------------------------------------------------
    private enum ExecutorType {
        /**
         * ForkJoin线程池
         */
        FORKJOIN("ForkJoin") {
            @Override
            public ExecutorService getExecutor() {
                return new ForkJoinPool();
            }
        },
        /**
         * 普通线程池
         */
        THREADPOOL("ThreadPool") {
            @Override
            public ExecutorService getExecutor() {
                return new ThreadPoolExecutor(0, SysUtils.getSuitableThreadNum(), 60L, TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(), new SimpleThreadFactory("default-thread-manager"));
            }
        };
        private String name;

        ExecutorType(String name) {
            this.name = name;
        }

        abstract ExecutorService getExecutor();

        static ExecutorType getByName(String name) {
            for (ExecutorType type : values()) {
                if (type.getName().toLowerCase().equals(name.toLowerCase())) {
                    return type;
                }
            }

            throw new UnknownExecutorTypeException("unknown executor type '" + name + "'");
        }

        String getName() {
            return name;
        }

        private static class UnknownExecutorTypeException extends RuntimeException {
            public UnknownExecutorTypeException(String message) {
                super(message);
            }
        }
    }

    private static ScheduledExecutorService getDefaultScheduledExecutor(){
        return new ScheduledThreadPoolExecutor(SysUtils.getSuitableThreadNum(),
                new SimpleThreadFactory("default-schedule-thread-manager"));
    }
}
