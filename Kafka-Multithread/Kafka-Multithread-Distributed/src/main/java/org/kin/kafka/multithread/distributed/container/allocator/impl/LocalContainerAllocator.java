package org.kin.kafka.multithread.distributed.container.allocator.impl;

import org.kin.kafka.multithread.distributed.container.allocator.ContainerAllocator;
import org.kin.kafka.multithread.distributed.node.ContainerContext;
import org.kin.kafka.multithread.distributed.node.Node;
import org.kin.kafka.multithread.distributed.node.NodeContext;
import org.kin.kafka.multithread.distributed.node.config.NodeConfig;
import org.kin.kafka.multithread.domain.HealthReport;
import org.kin.kafka.multithread.protocol.distributed.ContainerMasterProtocol;
import org.kin.kafka.multithread.rpc.factory.RPCFactories;
import org.kin.kafka.multithread.utils.HostUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by huangjianqin on 2017/9/19.
 *
 * 同一节点,不同jvm container的分配
 * 根据container的状态进行container的选择
 * 或者
 * 选择启动新的Container
 */
public class LocalContainerAllocator implements ContainerAllocator {
    private static final Logger log = LoggerFactory.getLogger("Node");

    private final Map<Long, ContainerMasterProtocol> id2Container;
    private final Properties nodeConfig;

    private Map<Long, HealthReport> id2HealthReport;
    private Map<Long, Long> id2SelectTimes;
    private Map<Long, Integer> id2IdleTimes;

    public LocalContainerAllocator(Map<Long, ContainerMasterProtocol> id2Container, Properties nodeConfig) {
        this.id2Container = id2Container;
        this.nodeConfig = nodeConfig;
    }

    @Override
    public void init() {
        this.id2HealthReport = new HashMap<>();
        this.id2SelectTimes = new HashMap<>();
        this.id2IdleTimes = new ConcurrentHashMap<>();
    }

    @Override
    public ContainerMasterProtocol containerAllocate(ContainerContext containerContext, NodeContext nodeContext) {
        //获取空闲超时的Container列表
        Set<Long> idleContainers = new HashSet<>();
        for(Long containerId: id2IdleTimes.keySet()){
            long idleTime = id2IdleTimes.get(containerId) * Long.valueOf(nodeConfig.getProperty(NodeConfig.CONTAINER_HEALTHREPORT_INTERNAL));
            long containerIdleTimeout = id2HealthReport.get(containerId).getContainerIdleTimeout();
            if(idleTime > containerIdleTimeout){
                idleContainers.add(containerId);
            }
        }

        ContainerMasterProtocol selectedContainerClient = null;
        //根据container的状态进行container的选择
        //利用CPU,空闲内存和APP运行数权重计算出分值,再跟阈值比较进行分配或构造新container
        long selectedContainerId = getBestContainer();
        if(selectedContainerId != -1){
            log.info("depoly app on Container(id=" + selectedContainerId + ")");
            selectedContainerClient =  id2Container.get(selectedContainerId);

            //关闭空闲的container资源
            //移除被选中container
            idleContainers.remove(selectedContainerId);
            //重置选中container的空闲time
            id2IdleTimes.put(selectedContainerId, 0);
            //RPC通知空闲container关闭
            askIdleContainer2Close(idleContainers);
        }
        else{
            //不超过单节点可启动container数
            if(id2Container.size() < Node.CONTAINER_NUM_LIMIT){
                log.info("setup and run a new Container(id=" +
                        containerContext.getContainerId() + ", nodeId=" +
                        nodeContext.getNodeId() + ") container_idle_timeout=" +
                        containerContext.getIdleTimeout() + ", container_healthreport_internal=" +
                        containerContext.getReportInternal() + ", protocol_port=" +
                        containerContext.getProtocolPort());
                //或者
                //选择启动新的Container
                StringBuilder args = new StringBuilder();
                args.append(" -D").append("containerId").append("=").append(containerContext.getContainerId())
                        .append(" -D").append("containerProtocolPort").append("=").append(containerContext.getProtocolPort())
                        .append(" -D").append(NodeConfig.CONTAINER_IDLETIMEOUT).append("=").append(containerContext.getIdleTimeout())
                        .append(" -D").append(NodeConfig.CONTAINER_HEALTHREPORT_INTERNAL).append("=").append(containerContext.getReportInternal())
                        .append(" -D").append("nodeId").append("=").append(nodeContext.getNodeId())
                        .append(" -D").append(NodeConfig.NODE_PROTOCOL_PORT).append("=").append(nodeContext.getProtocolPort());

                try {
                    Runtime.getRuntime().exec("java -jar Kafka-Multithread-Distributed.jar ContainerImpl " + args.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //构建与cotainer的RPC接口
                ContainerMasterProtocol containerClient = RPCFactories.instance().clientWithoutRegistry(ContainerMasterProtocol.class, HostUtils.localhost(), containerContext.getProtocolPort());
                id2Container.put(containerContext.getContainerId(), containerClient);

                selectedContainerClient = containerClient;
            }
            else{
                log.warn("need to start a new Container,but doesn't enough resource to setup and run a Container, refuse to run app");
            }
        }
        if(selectedContainerId != -1){
            //container选中次数++
            id2SelectTimes.put(selectedContainerId, id2SelectTimes.get(selectedContainerId) + 1);
        }

        return selectedContainerClient;
    }

    /**
     * RPC通知空闲container关闭
     * @param idleContainers
     */
    private void askIdleContainer2Close(Set<Long> idleContainers) {
        if(idleContainers == null){
            return;
        }

        for(Long idleContainerId: idleContainers){
            log.info("container(id=" + idleContainerId + ") is idle timeout, ask it to close");
            id2Container.remove(idleContainerId).close();
            containerClosed(idleContainerId);
        }
    }

    /**
     * 获得最适合分配的container
     * 如果找到返回containerId
     * 否则返回-1
     */
    private long getBestContainer(){
        //复制视图
        List<HealthReport> healthReportView = new ArrayList<>();
        for(HealthReport healthReport: id2HealthReport.values().toArray(new HealthReport[1])){
            if(healthReport.getContainerId() == Node.NODE_JVM_CONTAINER){
                //移除与Node同一JVM的Container
                continue;
            }

            try {
                healthReportView.add((HealthReport) healthReport.clone());
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        }

        Map<Long, Double> rates = compuateRate(healthReportView);

        //选择平均评分达到平均每项排名前80%
        double bestRate = healthReportView.size() * 0.8 * 4;
        for(Map.Entry<Long, Double> entry: rates.entrySet()){
            if(entry.getValue() >= bestRate){
                return entry.getKey();
            }
        }
        
        return -1;
    }

    private Map<Long, Double> compuateRate(List<HealthReport> healthReportView){
        Map<Long, Double> rates = new HashMap<>();

        //统计可用CPU排名
        healthReportView.sort(new Comparator<HealthReport>() {
            @Override
            public int compare(HealthReport o1, HealthReport o2) {
                return Integer.compare(o1.getAvailableProcessors(), o2.getAvailableProcessors());
            }
        });
        addRate(rates, healthReportView);

        //统计堆内存空闲比例排名
        healthReportView.sort(new Comparator<HealthReport>() {
            @Override
            public int compare(HealthReport o1, HealthReport o2) {
                return Double.compare(1.0 * o1.getFreeMemory() / o1.getTotalMemory(), 1.0 * o2.getFreeMemory() / o2.getTotalMemory());
            }
        });
        addRate(rates, healthReportView);

        //统计总堆内存排名
        healthReportView.sort(new Comparator<HealthReport>() {
            @Override
            public int compare(HealthReport o1, HealthReport o2) {
                return Double.compare(o1.getTotalMemory(), o2.getTotalMemory());
            }
        });
        addRate(rates, healthReportView);

        //统计Container选中次数排名,倒序,启动app越多,越值得信任,越优先选择
        healthReportView.sort(new Comparator<HealthReport>() {
            @Override
            public int compare(HealthReport o1, HealthReport o2) {
                return Long.compare(id2SelectTimes.getOrDefault(o1.getContainerId(), Long.MAX_VALUE),
                        id2SelectTimes.getOrDefault(o2.getContainerId(), Long.MAX_VALUE));
            }
        });
        addRate(rates, healthReportView);

        return rates;
    }

    private void addRate(Map<Long, Double> rates, List<HealthReport> rank){
        for(int i = 0; i < rank.size(); i++){
            HealthReport healthReport = rank.get(i);
            long containerId = healthReport.getContainerId();
            if(rates.containsKey(containerId)){
                rates.put(containerId, rates.get(containerId) + i + 1);
            }
            else{
                rates.put(containerId, i + 1.0);
            }
        }
    }

    @Override
    public void updateContainerStatus(HealthReport healthReport) {
        Long containerId = healthReport.getContainerId();

        id2HealthReport.put(containerId, healthReport);

        if(healthReport.getAppNums() == 0){
            if(id2IdleTimes.containsKey(containerId)){
                id2IdleTimes.put(containerId, id2IdleTimes.get(containerId) + 1);
            }
            else{
                id2IdleTimes.put(containerId, 1);
            }
        }
        else{
            id2IdleTimes.put(containerId, 0);
        }
    }

    @Override
    public void containerClosed(long containerId) {
        id2IdleTimes.remove(containerId);
        id2SelectTimes.remove(containerId);
        id2HealthReport.remove(containerId);
        id2Container.remove(containerId);
    }

    @Override
    public void close() {
        for(ContainerMasterProtocol container: id2Container.values()){
            container.close();
        }
    }
}
