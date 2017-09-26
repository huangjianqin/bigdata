package org.kin.kafka.multithread.distributed.container;

import org.kin.kafka.multithread.api.MultiThreadConsumerManager;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.core.Application;
import org.kin.kafka.multithread.distributed.AppStatus;
import org.kin.kafka.multithread.distributed.node.ContainerContext;
import org.kin.kafka.multithread.distributed.node.NodeContext;
import org.kin.kafka.multithread.domain.ConfigResultRequest;
import org.kin.kafka.multithread.domain.HealthReport;
import org.kin.kafka.multithread.protocol.distributed.ContainerMasterProtocol;
import org.kin.kafka.multithread.protocol.distributed.NodeMasterProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by huangjianqin on 2017/9/12.
 * Application容器,多JVM运行,充分利用同一节点的计算和存储资源
 */
public abstract class Container implements ContainerMasterProtocol {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    protected long containerId;
    protected long idleTimeout;
    protected long reportInternal;
    protected int containerMasterProtocolPort;
    protected int nodeMasterProtocolPort;
    //所属NodeId
    protected long belong2;
    protected NodeMasterProtocol nodeMasterProtocol;

    private long lastCommunicateTime = System.currentTimeMillis();
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("Container scheduled thread");
            return thread;
        }
    });

    //在另外的线程执行配置更新
    //多线程部署app,保证,同一appName按顺序执行部署命令
    //最多5条线程
    private ThreadPoolExecutor appDeployPool;
    //app配置队列
    private Map<String, Queue<Callable>> nQueue;
    //在线程池中执行或等待执行部署的appName
    private Set<String> deployingAppNames;

    protected MultiThreadConsumerManager appManager = MultiThreadConsumerManager.instance();

    protected Container(ContainerContext containerContext, NodeContext nodeContext) {
        this.idleTimeout = containerContext.getIdleTimeout();
        this.containerMasterProtocolPort = containerContext.getProtocolPort();
        this.nodeMasterProtocolPort = nodeContext.getProtocolPort();
        this.reportInternal = containerContext.getReportInternal();
    }

    public abstract void doStart();
    public abstract void doClose();

    public void start(){
        log.info("container(id=" + containerId + ", nodeId=" + belong2 + ") starting");
        //启动定时汇报心跳线程
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                healthReport();
            }
        }, 0, reportInternal, TimeUnit.MILLISECONDS);

        //定时检查是否空闲超时
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if(appManager.getAppSize() == 0){
                    if(System.currentTimeMillis() - lastCommunicateTime >= idleTimeout){
                        close();
                    }
                }
            }
        }, 0, idleTimeout, TimeUnit.MILLISECONDS);

        this.appDeployPool = new ThreadPoolExecutor(5, 5, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        this.appDeployPool.allowCoreThreadTimeOut(true);
        this.nQueue = new HashMap<>();
        this.deployingAppNames = Collections.synchronizedSet(new HashSet<>());

        doStart();
        log.info("container(id=" + containerId + ", nodeId=" + belong2 + ") started");

    }

    public void close(){
        log.info("container(id=" + containerId + ", nodeId=" + belong2 + ") closing");
        //先通知Node,但不关闭通信,为了避免再次分配Application到当前的Container,再自行关闭
        nodeMasterProtocol.closeContainer(containerId);
        scheduledExecutorService.shutdownNow();
        appDeployPool.shutdownNow();
        doClose();
        log.info("container(id=" + containerId + ", nodeId=" + belong2 + ") closed");
    }

    private void healthReport(){
        log.info("container(id=" + containerId + ", nodeId=" + belong2 + ") do health report");
        HealthReport healthReport = new HealthReport(appManager.getAppSize(), containerId);
        nodeMasterProtocol.report(healthReport);
    }

    @Override
    public Boolean updateConfig(List<Properties> configs) {
        lastCommunicateTime = System.currentTimeMillis();

        log.info("got " + configs.size() + " configs");
        log.info("deploy or close app...");
        //可考虑添加黑名单!!!!
        for(Properties config: configs){
            String appName = config.getProperty(AppConfig.APPNAME);

            AppStatus appStatus = AppStatus.getByStatusDesc(config.getProperty(AppConfig.APPSTATUS));
            //以后可能会根据返回值判断是否需要回滚配置更新
            Callable callable = null;
            switch (appStatus){
                case RUN:
                    if(!appManager.containsAppName(appName)){
                        callable = new RunConfigCallable(config);
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' already runned");
                    }
                    break;
                case UPDATE:
                    if(appManager.containsAppName(appName)){
                        callable = new UpdateConfigCallable(config);
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' doesn't runned");
                    }
                    break;
                case CLOSE:
                    if(!appManager.containsAppName(appName)){
                        callable = new CloseConfigCallable(config);
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' already runned");
                    }
                    break;
                case RESTART:
                    if(appManager.containsAppName(appName)){
                        callable = new RestartConfigCallable(config);
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' doesn't runned");
                    }
                    break;
            }
            deployApp(callable, appName);
        }
        log.info("deploy or close app finished");
        return true;
    }

    /**
     *
     * @param callable
     * @param appName
     */
    private void deployApp(Callable callable, String appName){
        if(deployingAppNames.contains(appName)){
            offerQueue(callable, appName);
        }
        else{
            if(nQueue.containsKey(appName)){
                if(nQueue.get(appName).size() == 0 && !deployingAppNames.contains(appName)){
                    //先抢占,再启动
                    if(deployingAppNames.add(appName)){
                        appDeployPool.submit(callable);
                    }
                    else{
                        offerQueue(callable, appName);
                    }
                }
                else {
                    offerQueue(callable, appName);
                }
            }
            else{
                //先抢占,再启动
                if(deployingAppNames.add(appName)){
                    appDeployPool.submit(callable);
                }
                else{
                    offerQueue(callable, appName);
                }
            }
        }
    }

    /**
     *
     * @param callable
     * @param appName
     */
    private void offerQueue(Callable callable, String appName){
        if(nQueue.containsKey(appName)){
            nQueue.get(appName).offer(callable);
        }
        else {
            synchronized (nQueue){
                if(nQueue.containsKey(appName)){
                    nQueue.get(appName).offer(callable);
                }
                else{
                    Queue<Callable> queue = new LinkedList<>();
                    queue.offer(callable);
                    nQueue.put(appName, queue);
                }
            }
        }
    }

    /**
     *
     * @param appName
     */
    private void updateQueue(String appName){
        if(nQueue.containsKey(appName) && nQueue.get(appName) != null && nQueue.get(appName).size() > 1){
            //已抢占,不用再次抢占
            appDeployPool.submit(nQueue.get(appName).poll());
        }
        else{
            deployingAppNames.remove(appName);
        }
    }

    public long getContainerId() {
        return containerId;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public int getContainerMasterProtocolPort() {
        return containerMasterProtocolPort;
    }

    public int getNodeMasterProtocolPort() {
        return nodeMasterProtocolPort;
    }

    public long getBelong2() {
        return belong2;
    }

    private abstract class DeployConfigCallable<V> implements Callable<V>{
        protected Properties config;
        protected String appName;

        public DeployConfigCallable(Properties config) {
            this.config = config;
            this.appName = config.getProperty(AppConfig.APPNAME);
        }

        @Override
        public V call() throws Exception {
            try{
                V result =  action();
                nodeMasterProtocol.commitConfigResultRequest(new ConfigResultRequest(appName, true, System.currentTimeMillis(), null));
                return result;
            }catch (Exception e){
                e.printStackTrace();
                log.warn(appName + "deploy has something wrong, throw " + e.getCause() + " exception and mark '" + e.getMessage() + "' message");
                nodeMasterProtocol.commitConfigResultRequest(new ConfigResultRequest(appName, false, System.currentTimeMillis(), e));
            }
            finally {
                updateQueue(appName);
            }

            return null;
        }

        public abstract V action();
    }

    private class RunConfigCallable<V> extends DeployConfigCallable<V>{

        public RunConfigCallable(Properties config) {
            super(config);
        }

        @Override
        public V action() {
            log.info("runing app '" + appName + "'...");
            Application application = MultiThreadConsumerManager.instance().newApplication(config);
            application.start();
            log.info("app '" + appName + "' runned");
            return null;
        }
    }

    private class UpdateConfigCallable<V> extends DeployConfigCallable<V>{

        public UpdateConfigCallable(Properties config) {
            super(config);
        }

        @Override
        public V action() {
            log.info("reconfig app '" + appName + "'...");
            appManager.reConfig(config);
            log.info("app '" + appName + "' reconfiged");
            return null;
        }
    }

    private class CloseConfigCallable<V> extends DeployConfigCallable<V>{

        public CloseConfigCallable(Properties config) {
            super(config);
        }

        @Override
        public V action() {
            log.info("app '" + appName + "' closing...");
            appManager.shutdownApp(appName);
            log.info("app '" + appName + "' closed");
            return null;
        }
    }

    private class RestartConfigCallable<V> extends DeployConfigCallable<V>{

        public RestartConfigCallable(Properties config) {
            super(config);
        }

        @Override
        public V action() {
            log.info("restart app '" + appName + "'...");
            appManager.shutdownApp(appName);
            Application newApplication = MultiThreadConsumerManager.instance().newApplication(config);
            newApplication.start();
            log.info("app '" + appName + "' restarted");
            return null;
        }
    }
}
