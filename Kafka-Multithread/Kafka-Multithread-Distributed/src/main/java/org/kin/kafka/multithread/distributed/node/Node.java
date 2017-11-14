package org.kin.kafka.multithread.distributed.node;

import org.apache.log4j.Level;
import org.kin.framework.log.Log4jLoggerBinder;
import org.kin.framework.utils.ExceptionUtils;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.distributed.configcenter.ConfigFetcher;
import org.kin.kafka.multithread.distributed.ChildRunModel;
import org.kin.kafka.multithread.distributed.container.impl.JVMContainer;
import org.kin.kafka.multithread.distributed.container.allocator.ContainerAllocator;
import org.kin.kafka.multithread.distributed.container.allocator.impl.LocalContainerAllocator;
import org.kin.kafka.multithread.distributed.node.config.NodeConfig;
import org.kin.kafka.multithread.distributed.utils.NodeConfigUtils;
import org.kin.kafka.multithread.domain.ConfigResultRequest;
import org.kin.kafka.multithread.domain.HealthReport;
import org.kin.kafka.multithread.protocol.app.ApplicationContextInfo;
import org.kin.kafka.multithread.protocol.distributed.ContainerMasterProtocol;
import org.kin.kafka.multithread.protocol.distributed.NodeMasterProtocol;
import org.kin.kafka.multithread.rpc.factory.RPCFactories;
import org.kin.kafka.multithread.utils.HostUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by huangjianqin on 2017/9/12.
 * 节点管理类,负责与配置中心通讯,并更新该节点下的所有container中的Application
 * 与ConfigFetcher整合
 *
 * 每个节点限制启动10个Container
 *
 * 充当Container资源的分配与关闭
 */
public class Node implements NodeMasterProtocol{
    static {log();}
    private static final Logger log = LoggerFactory.getLogger("Node");

    private static final Long nodeId = Long.valueOf(HostUtils.localhost().replaceAll("\\.", ""));
    public static final int CONTAINER_NUM_LIMIT = 10;
    public static final long NODE_JVM_CONTAINER = nodeId * CONTAINER_NUM_LIMIT;

    private ConfigFetcher configFetcher;
    private ContainerAllocator containerAllocator;
    //nodeId + 000 是与Node同一jvm的container
    private Map<Long, ContainerMasterProtocol> id2Container = new HashMap<>();

    private final Properties nodeConfig;
    private boolean isStopped = false;

    //配置队列
    private LinkedBlockingDeque<Properties> appConfigsQueue;
    private int containerAllocateRetry;

    public Node(){
        this("node.properties");
    }

    public Node(String configPath) {
        log();
        //加载配置
        this.nodeConfig = new Properties();
        try {
            this.nodeConfig.load(getClass().getClassLoader().getResourceAsStream(configPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("node config loaded");
        //校验配置
        NodeConfigUtils.oneNecessaryCheckAndFill(nodeConfig);
        log.info("config is Safe" + System.lineSeparator() + NodeConfigUtils.toString(nodeConfig));
    }

    public Node(Properties nodeConfig) {
        log();
        this.nodeConfig = nodeConfig;
        log.info("node config loaded");
        //校验配置
        NodeConfigUtils.oneNecessaryCheckAndFill(nodeConfig);
        log.info("config is Safe" + System.lineSeparator() + NodeConfigUtils.toString(nodeConfig));
    }

    /**
     * 如果没有适合的logger使用api创建默认logger
     */
    private static void log(){
        String logger = "Node";
        if(!Log4jLoggerBinder.exist(logger)){
            String appender = "node";
            Log4jLoggerBinder.create()
                    .setLogger(Level.INFO, logger, appender)
                    .setDailyRollingFileAppender(appender)
                    .setFile(appender, "/tmp/kafka-multithread/distributed/node.log")
                    .setDatePattern(appender)
                    .setAppend(appender, true)
                    .setThreshold(appender, Level.INFO)
                    .setPatternLayout(appender)
                    .setConversionPattern(appender)
                    .bind();
        }
    }

    public void init(){
        log.info("node initing...");

        this.appConfigsQueue =  new LinkedBlockingDeque<>();
        this.containerAllocator = new LocalContainerAllocator(id2Container, nodeConfig);
        this.containerAllocator.init();
        log.info("container allocator(" + containerAllocator.getClass().getName() + ") inited");

        String configCenterHost = nodeConfig.getProperty(NodeConfig.CONFIGCENTER_HOST);
        int configCenterPort = Integer.valueOf(nodeConfig.getProperty(NodeConfig.CONFIGCENTER_PORT));
        int nodeProtocolPort = Integer.valueOf(nodeConfig.getProperty(NodeConfig.NODE_PROTOCOL_PORT));
        int heartbeatInterval = Integer.valueOf(nodeConfig.getProperty(NodeConfig.CONFIGFETCHER_HEARTBEAT));

        this.configFetcher = new ConfigFetcher(configCenterHost, configCenterPort, appConfigsQueue, heartbeatInterval);
        this.configFetcher.start();

        this.containerAllocateRetry = Integer.valueOf(nodeConfig.getProperty(NodeConfig.CONTAINER_ALLOCATE_RETRY));

        RPCFactories.serviceWithoutRegistry(NodeMasterProtocol.class, this, nodeProtocolPort);
        log.info("NodeMasterProtocol rpc interface inited, binding on {}:{}", HostUtils.localhost(), nodeProtocolPort);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                configFetcher.close();
                Node.this.close();
            }
        }));
        log.info("node inited");
    }

    public void start(){
        log.info("node started. ready to maintenance app status");
        long containerIdleTimeout = Long.valueOf(nodeConfig.getProperty(NodeConfig.CONTAINER_IDLETIMEOUT));
        long containerHealthReportInternal = Long.valueOf(nodeConfig.getProperty(NodeConfig.CONTAINER_HEALTHREPORT_INTERNAL));
        int nodeProtocolPort = Integer.valueOf(nodeConfig.getProperty(NodeConfig.NODE_PROTOCOL_PORT));
        int containerInitProtocolPort = Integer.valueOf(nodeConfig.getProperty(NodeConfig.CONTAINER_PROTOCOL_INITPORT));
        NodeContext nodeContext = new NodeContext(HostUtils.localhost(), nodeId, nodeProtocolPort);
        int nowContainerAllocateRetry = 0;
        while(!isStopped && !Thread.currentThread().isInterrupted()){
            try {
                Properties newConfig = appConfigsQueue.take();

                ChildRunModel runModel = ChildRunModel.getByName(newConfig.getProperty(AppConfig.APP_CHILD_RUN_MODEL));

                ContainerMasterProtocol containerMasterProtocol = null;
                ContainerContext containerContext = null;

                switch (runModel){
                    case JVM:
                        if(id2Container.containsKey(NODE_JVM_CONTAINER)){
                            log.debug("got jvm app and jvm Container has runned, just to setup app");
                            containerMasterProtocol = id2Container.get(NODE_JVM_CONTAINER);
                        }
                        else{
                            log.info("got jvm app but jvm Container has not runned, so to run jvm Container and then run app");
                            //不使用默认的构造
                            containerContext = new ContainerContext(
                                    nodeId * CONTAINER_NUM_LIMIT,
                                    getContainerProtocolPort(containerInitProtocolPort, NODE_JVM_CONTAINER),
                                    containerIdleTimeout,
                                    containerHealthReportInternal);
                            JVMContainer jvmContainer = new JVMContainer(containerContext, nodeContext, this);
                            jvmContainer.start();
                            containerMasterProtocol = jvmContainer;
                            id2Container.put(containerContext.getContainerId(), containerMasterProtocol);
                        }
                        break;
                    case NODE:
                        log.info("got node app");
                        //获取一个可用containerId
                        long tmp = getContainerId();
                        containerContext = new ContainerContext(
                                tmp,
                                getContainerProtocolPort(containerInitProtocolPort, tmp),
                                containerIdleTimeout,
                                containerHealthReportInternal);
                        //如果能再现有container中运行app,则返回现有container的containerMasterProtocol接口
                        //否则,利用containerContext参数创建新的container,并返回该container的containerMasterProtocol接口
                        containerMasterProtocol = containerAllocator.containerAllocate(containerContext, nodeContext);
                        break;
                    default:
                        throw new IllegalStateException("unknown AppChildRunModel '" + newConfig.getProperty(AppConfig.APP_CHILD_RUN_MODEL) + "'");
                }
                try{
                    //更新配置或运行新实例
                    containerMasterProtocol.updateConfig(Collections.singletonList(newConfig));
                }catch (Exception ex){
                    ExceptionUtils.log(ex);
                    nowContainerAllocateRetry ++;
                    if(nowContainerAllocateRetry <= containerAllocateRetry){
                        log.info("{} times to retry to allocate a container for app '{}'", nowContainerAllocateRetry, newConfig.getProperty(AppConfig.APPNAME));
                        //把配置重新插入队列头,这样当前重试次数仍然对该配置有效
                        appConfigsQueue.offerFirst(newConfig);
                    }
                    else {
                        nowContainerAllocateRetry = 0;
                        configFetcher.configFail(Collections.singletonList(newConfig));
                    }
                }

            } catch (InterruptedException e) {
                log.error("node '{}' run wrong", nodeId);
                this.close();
            }
        }
        log.info("node closed");
    }

    public void close(){
        log.info("node closing...");
        this.isStopped = false;
        if(this.configFetcher != null){
            this.configFetcher.close();
        }
        if(this.containerAllocator != null){
            this.containerAllocator.close();
        }
    }

    @Override
    public void report(HealthReport report) {
        containerAllocator.updateContainerStatus(report);
    }

    @Override
    public void commitConfigResultRequest(ConfigResultRequest configResultRequest) {
        ApplicationContextInfo applicationContextInfo = configResultRequest.getApplicationContextInfo();
        boolean isSucceed = configResultRequest.isSucceed();
        if(isSucceed){
            configFetcher.appSucceed(Collections.singletonList(applicationContextInfo));
        }
        else{
            configFetcher.appFail(Collections.singletonList(applicationContextInfo));
        }
    }

    @Override
    public void containerClosed(long containId) {
        containerAllocator.containerClosed(containId);
    }

    private long getContainerId(){
        long min = nodeId * CONTAINER_NUM_LIMIT + 1;
        long max = (nodeId + 1) * CONTAINER_NUM_LIMIT - 1;
        for(long i = min; i < max; i++){
            if(!id2Container.containsKey(i)){
                return i;
            }
        }
        throw new IllegalStateException("greater than the number of Container per node");
    }

    private int getContainerProtocolPort(int containerInitProtocolPort, long containerId){
        return containerInitProtocolPort + (int)(containerId - nodeId * CONTAINER_NUM_LIMIT);
    }
}
