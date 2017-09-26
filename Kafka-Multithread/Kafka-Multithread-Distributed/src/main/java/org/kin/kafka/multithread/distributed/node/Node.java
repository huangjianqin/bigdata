package org.kin.kafka.multithread.distributed.node;

import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.configcenter.ConfigFetcher;
import org.kin.kafka.multithread.distributed.ChildRunModel;
import org.kin.kafka.multithread.distributed.container.impl.JVMContainer;
import org.kin.kafka.multithread.distributed.container.allocator.ContainerAllocator;
import org.kin.kafka.multithread.distributed.container.allocator.impl.LocalContainerAllocator;
import org.kin.kafka.multithread.distributed.node.config.NodeConfig;
import org.kin.kafka.multithread.distributed.utils.NodeConfigUtils;
import org.kin.kafka.multithread.domain.ConfigResultRequest;
import org.kin.kafka.multithread.domain.HealthReport;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by huangjianqin on 2017/9/12.
 * 节点管理类,负责与配置中心通讯,并更新该节点下的所有container中的Application
 * 与ConfigFetcher整合
 *
 * 每个节点限制启动10个Container
 *
 * 暂时没有解决当app决定分发到某container后,container还没有接受就自动关闭了导致app启动失败的场景
 */
public class Node implements NodeMasterProtocol{
    private static final Logger log = LoggerFactory.getLogger(Node.class);

    private static final Long nodeId = Long.valueOf(HostUtils.localhost().replaceAll(".", ""));
    public static final int CONTAINER_NUM_LIMIT = 10;
    public static final long NODE_JVM_CONTAINER = nodeId * CONTAINER_NUM_LIMIT;

    private ConfigFetcher configFetcher;
    private ContainerAllocator containerAllocator;
    //nodeId + 000 是与Node同一jvm的container
    private Map<Long, ContainerMasterProtocol> id2Container = new HashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    private Properties nodeConfig;
    private boolean isStopped = false;

    private LinkedBlockingQueue<Properties> appConfigs;

    public Node() {
        //加载配置
        this.nodeConfig = new Properties();
        try {
            this.nodeConfig.load(getClass().getClassLoader().getResourceAsStream("application.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("node config loaded");
        //校验配置
        NodeConfigUtils.oneNecessaryCheckAndFill(nodeConfig);
        log.info("config is Safe" + System.lineSeparator() + NodeConfigUtils.toString(nodeConfig));
    }

    public Node(Properties nodeConfig) {
        this.nodeConfig = nodeConfig;
        log.info("node config loaded");
        //校验配置
        NodeConfigUtils.oneNecessaryCheckAndFill(nodeConfig);
        log.info("config is Safe" + System.lineSeparator() + NodeConfigUtils.toString(nodeConfig));
    }

    public void init(){
        log.info("node initing...");

        this.appConfigs =  new LinkedBlockingQueue<>();
        this.containerAllocator = new LocalContainerAllocator(id2Container);
        this.containerAllocator.init();
        log.info("container allocator(" + containerAllocator.getClass().getName() + ") inited");

        String configCenterHost = nodeConfig.getProperty(AppConfig.CONFIGCENTER_HOST);
        int configCenterPort = Integer.valueOf(nodeConfig.getProperty(AppConfig.CONFIGCENTER_PORT));
        int nodeProtocolPort = Integer.valueOf(nodeConfig.getProperty(NodeConfig.NODE_PROTOCOL_PORT));

        this.configFetcher = new ConfigFetcher(configCenterHost, configCenterPort, appConfigs);
        this.configFetcher.start();

        RPCFactories.serviceWithoutRegistry(NodeMasterProtocol.class, this, nodeProtocolPort);
        log.info("NodeMasterProtocol rpc interface inited, binding " + nodeProtocolPort + " port");

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                configFetcher.close();
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
        NodeContext nodeContext = new NodeContext(nodeId, nodeProtocolPort);
        while(!isStopped && !Thread.currentThread().isInterrupted()){
            try {
                Properties newConfig = appConfigs.take();

                ChildRunModel runModel = ChildRunModel.getByName(newConfig.getProperty(AppConfig.APP_CHILD_RUN_MODEL));

                ContainerMasterProtocol containerMasterProtocol = null;
                ContainerContext containerContext = null;

                lock.lock();
                switch (runModel){
                    case JVM:
                        if(id2Container.containsKey(NODE_JVM_CONTAINER)){
                            log.info("got jvm app and jvm Container has runned, just to setup app");
                            containerMasterProtocol = id2Container.get(NODE_JVM_CONTAINER);
                        }
                        else{
                            log.info("got jvm app but jvm Container has not runned, so to run jvm Container and then run app");
                            //不使用默认的构造
                            containerContext = new ContainerContext(
                                    nodeId * CONTAINER_NUM_LIMIT,
                                    getContainerProtocolPort(containerInitProtocolPort, NODE_JVM_CONTAINER),
                                    containerContext.getIdleTimeout(),
                                    containerHealthReportInternal);
                            containerMasterProtocol = new JVMContainer(containerContext, nodeContext, this);
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
                        if(containerMasterProtocol == null){
                            configFetcher.configFailConfigs(Collections.singletonList(newConfig));
                        }
                        break;
                    default:
                        throw new IllegalStateException("unknown AppChildRunModel '" + newConfig.getProperty(AppConfig.APP_CHILD_RUN_MODEL) + "'");
                }
                lock.unlock();
                //更新配置或运行新实例
                containerMasterProtocol.updateConfig(Collections.singletonList(newConfig));

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("node closed");
    }

    public void close(){
        log.info("node closing...");
        this.isStopped = false;
        this.configFetcher.close();
        this.containerAllocator.close();
    }

    @Override
    public Boolean closeContainer(long containerId) {
        lock.lock();
        log.info("node remove container(id=" + containerId + ") info and close container");
        boolean result = id2Container.remove(containerId) == null;
        lock.unlock();
        return result;
    }

    @Override
    public void report(HealthReport report) {
        containerAllocator.updateContainerStatus(report);
    }

    @Override
    public void commitConfigResultRequest(ConfigResultRequest configResultRequest) {
        String appName = configResultRequest.getAppName();
        boolean isSucceed = configResultRequest.isSucceed();
        if(isSucceed){
            configFetcher.configSucceedAppNames(Collections.singletonList(appName));
        }
        else{
            configFetcher.configFailAppNames(Collections.singletonList(appName));
        }
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

    public static void main(String[] args) {
        Node node = new Node();
        node.init();
        node.start();
    }
}
