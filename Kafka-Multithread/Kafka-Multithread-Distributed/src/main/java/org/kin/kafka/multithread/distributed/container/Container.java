package org.kin.kafka.multithread.distributed.container;

import org.apache.log4j.Level;
import org.kin.framework.concurrent.PartitionTaskExecutors;
import org.kin.framework.log.Log4jLoggerBinder;
import org.kin.kafka.multithread.api.MultiThreadConsumerManager;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.api.Application;
import org.kin.kafka.multithread.distributed.AppStatus;
import org.kin.kafka.multithread.distributed.node.ContainerContext;
import org.kin.kafka.multithread.distributed.node.NodeContext;
import org.kin.kafka.multithread.domain.ConfigResultRequest;
import org.kin.kafka.multithread.domain.HealthReport;
import org.kin.kafka.multithread.protocol.app.ApplicationContextInfo;
import org.kin.kafka.multithread.protocol.distributed.ContainerMasterProtocol;
import org.kin.kafka.multithread.protocol.distributed.NodeMasterProtocol;
import org.kin.kafka.multithread.utils.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by huangjianqin on 2017/9/12.
 * Application容器,多JVM运行,充分利用同一节点的计算和存储资源
 */
public abstract class Container implements ContainerMasterProtocol {
    static {log();}
    protected final Logger log = LoggerFactory.getLogger("Container");

    private NodeContext nodeContext;
    protected long containerId;
    protected long idleTimeout;
    protected long reportInternal;
    protected int containerMasterProtocolPort;
    protected int nodeMasterProtocolPort;
    //所属NodeId
    protected long belong2;
    protected NodeMasterProtocol nodeMasterProtocol;

    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, new ThreadFactory() {
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
    private PartitionTaskExecutors partitionTaskExecutors;

    protected MultiThreadConsumerManager appManager = MultiThreadConsumerManager.instance();

    protected Container(ContainerContext containerContext, NodeContext nodeContext) {
        this.nodeContext = nodeContext;
        this.idleTimeout = containerContext.getIdleTimeout();
        this.containerMasterProtocolPort = containerContext.getProtocolPort();
        this.nodeMasterProtocolPort = nodeContext.getProtocolPort();
        this.reportInternal = containerContext.getReportInternal();

        log();
    }

    /**
     * 如果没有适合的logger使用api创建默认logger
     */
    private static void log(){
        String logger = "Container";
        if(!Log4jLoggerBinder.exist(logger)){
            String appender = "container";
            Log4jLoggerBinder.create()
                    .setLogger(Level.INFO, logger, appender)
                    .setDailyRollingFileAppender(appender)
                    .setFile(appender, "/tmp/kafka-multithread/distributed/container${containerId}.log")
                    .setDatePattern(appender)
                    .setAppend(appender, true)
                    .setThreshold(appender, Level.INFO)
                    .setPatternLayout(appender)
                    .setConversionPattern(appender)
                    .bind();
        }
    }

    public abstract void doStart();
    public abstract void doClose();

    public void start(){
        log.info("container(id=" + containerId + ", nodeId=" + belong2 + ") starting");

        this.partitionTaskExecutors = new PartitionTaskExecutors(5);
        this.partitionTaskExecutors.init();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                Container.this.close();
            }
        }));

        doStart();
        log.info("container(id=" + containerId + ", nodeId=" + belong2 + ") started");

        //启动定时汇报心跳线程
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                healthReport();
            }
        }, 0, reportInternal, TimeUnit.MILLISECONDS);
    }

    /**
     * 由Node负责关闭
     */
    @Override
    public void close(){
        log.info("container(id=" + containerId + ", nodeId=" + belong2 + ") closing");
        scheduledExecutorService.shutdownNow();
        partitionTaskExecutors.shutdown();
        //通知Node container关闭了
        nodeMasterProtocol.containerClosed(containerId);
        doClose();
        log.info("container(id=" + containerId + ", nodeId=" + belong2 + ") closed");
    }

    private void healthReport(){
        log.debug("container(id=" + containerId + ", nodeId=" + belong2 + ") do health report");
        HealthReport healthReport = new HealthReport(containerId, appManager.getAppSize(), idleTimeout);
        nodeMasterProtocol.report(healthReport);
    }

    /**
     * 保证配置应用按接受顺序进行,也就是说不会存在同一app同时配置不同的配置
     * @param configs
     * @return
     */
    @Override
    public Boolean updateConfig(List<Properties> configs) {
        log.info("got " + configs.size() + " configs");

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
                        //只拿可更新的配置,其余不变配置不管
                        Iterator<Map.Entry<Object, Object>> iterator = config.entrySet().iterator();
                        while(iterator.hasNext()){
                            Object key = iterator.next().getKey();
                            //new config 必须包含require 和 可以改变的 config key
                            if(AppConfig.REQUIRE_APPCONFIGS.contains(key)){
                                continue;
                            }
                            if(!AppConfig.CAN_RECONFIG_APPCONFIGS.contains(key)){
                                iterator.remove();
                            }
                        }


                        callable = new UpdateConfigCallable(config);
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' doesn't runned");
                    }
                    break;
                case CLOSE:
                    if(appManager.containsAppName(appName)){
                        callable = new CloseConfigCallable(config);
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' hasn't  runned");
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
            //成功部署,移除app运行状态
            config.remove(AppConfig.APPSTATUS);
        }
        return true;
    }

    /**
     *
     * @param callable
     * @param appName
     */
    private void deployApp(Callable callable, String appName){
        partitionTaskExecutors.execute(appName, callable);
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
        protected AppStatus appStatus;

        public DeployConfigCallable(Properties config, AppStatus appStatus) {
            this.config = config;
            this.appName = config.getProperty(AppConfig.APPNAME);
            this.appStatus = appStatus;
        }

        @Override
        public V call() throws Exception {
            ApplicationContextInfo applicationContextInfo = new ApplicationContextInfo();
            applicationContextInfo.setHost(nodeContext.getHost());
            applicationContextInfo.setAppName(appName);
            applicationContextInfo.setAppStatus(appStatus);
            try{
                V result =  action();
                nodeMasterProtocol.commitConfigResultRequest(new ConfigResultRequest(applicationContextInfo, true, System.currentTimeMillis(), null));
                return result;
            }catch (Exception e){
                ExceptionUtils.log(e);
                log.warn(appName + " deploy has something wrong, throw " + e.getCause() + " exception and mark '" + e.getMessage() + "' message");
                nodeMasterProtocol.commitConfigResultRequest(new ConfigResultRequest(applicationContextInfo, false, System.currentTimeMillis(), e));
                //考虑更新配置失败时,回滚
                //....
            }

            return null;
        }

        public abstract V action();
    }

    private class RunConfigCallable<V> extends DeployConfigCallable<V>{
        public RunConfigCallable(Properties config) {
            super(config, AppStatus.RUN);
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
            super(config, AppStatus.UPDATE);
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
            super(config, AppStatus.CLOSE);
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
            super(config, AppStatus.RESTART);
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
