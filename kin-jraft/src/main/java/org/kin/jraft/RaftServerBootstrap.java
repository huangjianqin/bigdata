package org.kin.jraft;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import org.apache.commons.io.FileUtils;
import org.kin.framework.utils.ExceptionUtils;
import org.kin.framework.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2021/11/7
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class RaftServerBootstrap implements Lifecycle<RaftServerOptions> {
    private static final Logger log = LoggerFactory.getLogger(RaftServerBootstrap.class);

    /** raft service */
    private RaftGroupService raftGroupService;
    /** raft node */
    private Node node;
    /** {@link com.alipay.sofa.jraft.StateMachine}实现 */
    private DefaultStateMachine sm;
    /** raft rpc server */
    private RpcServer raftRpcServer;
    /** 状态标识 */
    private boolean started;
    /** raft service */
    private RaftService raftService;
    /** raft service rpc server */
    private RpcServer raftServiceRpcServer;

    @Override
    public synchronized boolean init(RaftServerOptions opts) {
        if (started) {
            log.info("[RaftNode: {}] already started.", opts.getAddress());
            return true;
        }

        //初始化数据目录
        String dataDir = opts.getDataDir();
        try {
            FileUtils.forceMkdir(new File(dataDir));
        } catch (IOException e) {
            log.error("fail to make dir for dataDir {}.", dataDir);
            return false;
        }

        //init raft service
        //标识raft node和raft service是否使用同一个rpc server
        RaftServiceFactory raftServiceFactory = opts.getRaftServiceFactory();
        boolean isRaftNodeServiceSameRpcServer = false;
        String serviceAddress = opts.getServiceAddress();
        if (StringUtils.isBlank(serviceAddress)) {
            //默认raft node和raft service使用同一个rpc server
            serviceAddress = opts.getAddress();
        }
        if (serviceAddress.equalsIgnoreCase(opts.getAddress())) {
            isRaftNodeServiceSameRpcServer = true;
        }
        PeerId servicePeerId = new PeerId();
        if (!servicePeerId.parse(serviceAddress)) {
            throw new IllegalArgumentException("fail to parse servicePeerId: " + serviceAddress);
        }
        raftServiceRpcServer = RaftRpcServerFactory.createRaftRpcServer(servicePeerId.getEndpoint());
        raftService = raftServiceFactory.create(this, raftServiceRpcServer);

        //node options
        NodeOptions nodeOpts = new NodeOptions();

        StateMachineFactory stateMachineFactory = opts.getStateMachineFactory();
        if (Objects.nonNull(stateMachineFactory)) {
            sm = stateMachineFactory.create(this, raftService);
        } else {
            //default
            sm = new DefaultStateMachine(opts.getListeners());
        }
        sm.init(opts);

        nodeOpts.setFsm(sm);

        //cluster node address
        Configuration initialConf = new Configuration();
        if (!initialConf.parse(opts.getClusterAddresses())) {
            throw new IllegalArgumentException("fail to parse initConf: " + opts.getClusterAddresses());
        }
        nodeOpts.setInitialConf(initialConf);

        //设置Log存储路径
        nodeOpts.setLogUri(Paths.get(dataDir, "log").toString());
        //设置Metadata存储路径
        nodeOpts.setRaftMetaUri(Paths.get(dataDir, "meta").toString());
        if (nodeOpts.getSnapshotIntervalSecs() > 0) {
            //设置Snapshot存储路径
            nodeOpts.setSnapshotUri(Paths.get(dataDir, "snapshot").toString());
        }
        opts.setupOtherNodeOptions(nodeOpts);

        //group
        String groupId = opts.getGroupId();
        //current node peer
        PeerId serverId = new PeerId();
        if (!serverId.parse(opts.getAddress())) {
            throw new IllegalArgumentException("fail to parse serverId: " + opts.getAddress());
        }
        if (isRaftNodeServiceSameRpcServer) {
            raftRpcServer = raftServiceRpcServer;
        } else {
            //init raft rpc server
            raftRpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        }
        //init raft service
        raftGroupService = new RaftGroupService(groupId, serverId, nodeOpts, raftRpcServer);

        //start
        //raft node start
        node = raftGroupService.start();
        //raft server rpc server start
        if (!isRaftNodeServiceSameRpcServer()) {
            raftServiceRpcServer.init(null);
        }

        if (node != null) {
            started = true;
        }
        return started;
    }

    @Override
    public void shutdown() {
        if (!started) {
            return;
        }
        raftService.close();
        if (raftGroupService != null) {
            raftGroupService.shutdown();
            try {
                raftGroupService.join();
            } catch (InterruptedException e) {
                ExceptionUtils.throwExt(e);
            }
        }
        if (!isRaftNodeServiceSameRpcServer()) {
            raftServiceRpcServer.shutdown();
        }
        started = false;
        log.info("[RaftNode] shutdown successfully: {}.", this);
    }

    /**
     * raft node和raft service是否使用同一个rpc server
     */
    private boolean isRaftNodeServiceSameRpcServer() {
        return raftRpcServer.equals(raftServiceRpcServer);
    }

    /**
     * 添加{@link NodeStateChangeListener} list
     */
    public void addListeners(List<NodeStateChangeListener> listeners) {
        sm.addListeners(listeners);
    }

    /**
     * 添加{@link NodeStateChangeListener} list
     */
    public void addListeners(NodeStateChangeListener... listeners) {
        addListeners(Arrays.asList(listeners));
    }

    //getter
    public Node getNode() {
        return node;
    }

    public boolean isStarted() {
        return started;
    }

    public <NW extends DefaultStateMachine> NW getSm() {
        return (NW) sm;
    }

    public <S extends RaftService> S getRaftService() {
        return (S) raftService;
    }
}