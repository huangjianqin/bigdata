package org.kin.kafka.multithread.distributed.node;

import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.configcenter.ConfigFetcher;
import org.kin.kafka.multithread.distributed.ChildRunModel;
import org.kin.kafka.multithread.distributed.container.impl.JVMContainer;
import org.kin.kafka.multithread.distributed.container.allocator.ContainerAllocator;
import org.kin.kafka.multithread.distributed.container.allocator.impl.LocalContainerAllocator;
import org.kin.kafka.multithread.distributed.node.config.NodeConfig;
import org.kin.kafka.multithread.distributed.utils.NodeConfigUtils;
import org.kin.kafka.multithread.domain.HealthReport;
import org.kin.kafka.multithread.protocol.distributed.ContainerMasterProtocol;
import org.kin.kafka.multithread.protocol.distributed.NodeMasterProtocol;
import org.kin.kafka.multithread.rpc.factory.RPCFactories;
import org.kin.kafka.multithread.utils.HostUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by huangjianqin on 2017/9/12.
 * 节点管理类,负责与配置中心通讯,并更新该节点下的所有container中的Application
 * 与ConfigFetcher整合
 *
 * 每个节点限制启动10个Container
 */
public class Node implements NodeMasterProtocol{
    private static final Long nodeId = Long.valueOf(HostUtils.localhost().replaceAll(".", ""));
    public static final int CONTAINER_NUM_LIMIT = 10;
    public static final long NODE_JVM_CONTAINER = nodeId * CONTAINER_NUM_LIMIT;

    private ConfigFetcher configFetcher;
    private ContainerAllocator containerAllocator;
    //nodeId + 000 是与Node同一jvm的container
    private Map<Long, ContainerMasterProtocol> id2Container = new HashMap<>();
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
        //校验配置
        NodeConfigUtils.oneNecessaryCheckAndFill(nodeConfig);
    }

    public Node(Properties nodeConfig) {
        this.nodeConfig = nodeConfig;
        //校验配置
        NodeConfigUtils.oneNecessaryCheckAndFill(nodeConfig);
    }

    public void init(){
        //检查必要配置
        //检查配置格式
        //填充默认值

        this.appConfigs =  new LinkedBlockingQueue<>();
        this.containerAllocator = new LocalContainerAllocator(id2Container);
        this.containerAllocator.init();

        String configCenterHost = nodeConfig.getProperty(AppConfig.CONFIGCENTER_HOST);
        int configCenterPort = Integer.valueOf(nodeConfig.getProperty(AppConfig.CONFIGCENTER_PORT));
        int nodeProtocolPort = Integer.valueOf(nodeConfig.getProperty(NodeConfig.NODE_PROTOCOL_PORT));

        this.configFetcher = new ConfigFetcher(configCenterHost, configCenterPort, appConfigs);
        this.configFetcher.start();

        RPCFactories.serviceWithoutRegistry(NodeMasterProtocol.class, this, nodeProtocolPort);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                configFetcher.close();
            }
        }));
    }

    public void start(){
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
                //不一定使用,jvm模式会使用默认的containerId
                long tmp = getContainerId();
                ContainerContext containerContext = new ContainerContext(
                        tmp,
                        getContainerProtocolPort(containerInitProtocolPort, tmp),
                        containerIdleTimeout,
                        containerHealthReportInternal);
                switch (runModel){
                    case JVM:
                        if(id2Container.containsKey(NODE_JVM_CONTAINER)){
                            containerMasterProtocol = id2Container.get(NODE_JVM_CONTAINER);
                        }
                        else{
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
                        containerMasterProtocol = containerAllocator.containerAllocate(containerContext, nodeContext);
                        if(containerMasterProtocol == null){
                            configFetcher.configFail(Collections.singletonList(newConfig));
                        }
                        break;
                    default:
                        throw new IllegalStateException("unknown AppChildRunModel '" + newConfig.getProperty(AppConfig.APP_CHILD_RUN_MODEL) + "'");
                }
                //更新配置或运行新实例
                containerMasterProtocol.updateConfig(Collections.singletonList(newConfig));

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void close(){
        this.isStopped = false;
        this.configFetcher.close();
        this.containerAllocator.close();
    }

    @Override
    public Boolean closeContainer(long containerId) {
        return id2Container.remove(containerId) == null;
    }

    @Override
    public void report(HealthReport report) {
        containerAllocator.updateContainerStatus(report);
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
