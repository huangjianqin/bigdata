package org.kin.jraft;

import com.alipay.sofa.jraft.core.ElectionPriority;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.util.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * raft选举配置
 *
 * @author huangjianqin
 * @date 2021/11/7
 */
public class RaftServerOptions<NW extends DefaultStateMachine, S extends RaftService> {
    /** 数据存储目录 */
    private String dataDir;
    /** raft group id */
    private String groupId;
    /** ip:port */
    private String address;
    /** ip:port,ip:port,ip:port */
    private String clusterAddresses;
    /** {@link NodeStateChangeListener} list */
    private List<NodeStateChangeListener> listeners = new ArrayList<>();
    /** ip:port, raft service绑定地址 */
    private String serviceAddress;
    /** {@link RaftService}实现类构建逻辑 */
    private RaftServiceFactory<S> raftServiceFactory;
    /** 自定义{@link com.alipay.sofa.jraft.StateMachine}实现类构建逻辑 */
    private StateMachineFactory<NW, S> stateMachineFactory;
    /** 快照文件操作 */
    private SnapshotFileOpr<?> snapshotFileOpr = DefaultSnapshotFileOpr.INSTANCE;

    /** {@link NodeOptions#getElectionTimeoutMs()} */
    private int electionTimeoutMs = 1000;
    /** {@link NodeOptions#getElectionPriority()} */
    private int electionPriority = ElectionPriority.Disabled;
    /** {@link NodeOptions#getDecayPriorityGap()} */
    private int decayPriorityGap = 10;
    /** {@link NodeOptions#getLeaderLeaseTimeRatio()} */
    private int leaderLeaseTimeRatio = 90;
    /** {@link NodeOptions#getSnapshotIntervalSecs()} */
    private int snapshotIntervalSecs = 3600;
    /** {@link NodeOptions#getSnapshotLogIndexMargin()} */
    private int snapshotLogIndexMargin = 0;
    /** {@link NodeOptions#getCatchupMargin()} */
    private int catchupMargin = 1000;
    /** {@link NodeOptions#isFilterBeforeCopyRemote()} */
    private boolean filterBeforeCopyRemote = false;
    /** {@link NodeOptions#isDisableCli()} */
    private boolean disableCli = false;
    /** {@link NodeOptions#isSharedTimerPool()} */
    private boolean sharedTimerPool = false;
    /** {@link NodeOptions#getTimerPoolSize()} */
    private int timerPoolSize = Math.min(Utils.cpus() * 3, 20);
    /** {@link NodeOptions#getCliRpcThreadPoolSize()} */
    private int cliRpcThreadPoolSize = Utils.cpus();
    /** {@link NodeOptions#getRaftRpcThreadPoolSize()} */
    private int raftRpcThreadPoolSize = Utils.cpus() * 6;
    /** {@link NodeOptions#isEnableMetrics()} */
    private boolean enableMetrics = false;
    /** {@link NodeOptions#getSnapshotThrottle()} */
    private SnapshotThrottle snapshotThrottle;
    /** {@link NodeOptions#isSharedElectionTimer()} ()} */
    private boolean sharedElectionTimer = false;
    /** {@link NodeOptions#isSharedVoteTimer()} */
    private boolean sharedVoteTimer = false;
    /** {@link NodeOptions#isSharedStepDownTimer()} */
    private boolean sharedStepDownTimer = false;
    /** {@link NodeOptions#isSharedSnapshotTimer()} */
    private boolean sharedSnapshotTimer = false;

    private RaftServerOptions() {
    }

    void setupOtherNodeOptions(NodeOptions nodeOpts) {
        nodeOpts.setElectionTimeoutMs(electionTimeoutMs);
        nodeOpts.setElectionPriority(electionPriority);
        nodeOpts.setDecayPriorityGap(decayPriorityGap);
        nodeOpts.setLeaderLeaseTimeRatio(leaderLeaseTimeRatio);
        nodeOpts.setSnapshotIntervalSecs(snapshotIntervalSecs);
        nodeOpts.setSnapshotLogIndexMargin(snapshotLogIndexMargin);
        nodeOpts.setCatchupMargin(catchupMargin);
        nodeOpts.setFilterBeforeCopyRemote(filterBeforeCopyRemote);
        nodeOpts.setDisableCli(disableCli);
        nodeOpts.setSharedTimerPool(sharedTimerPool);
        nodeOpts.setTimerPoolSize(timerPoolSize);
        nodeOpts.setCliRpcThreadPoolSize(cliRpcThreadPoolSize);
        nodeOpts.setRaftRpcThreadPoolSize(raftRpcThreadPoolSize);
        nodeOpts.setEnableMetrics(enableMetrics);
        nodeOpts.setSnapshotThrottle(snapshotThrottle);
        nodeOpts.setSharedElectionTimer(sharedElectionTimer);
        nodeOpts.setSharedVoteTimer(sharedVoteTimer);
        nodeOpts.setSharedStepDownTimer(sharedStepDownTimer);
        nodeOpts.setSharedSnapshotTimer(sharedSnapshotTimer);
    }

    //getter
    public String getDataDir() {
        return dataDir;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getAddress() {
        return address;
    }

    public String getClusterAddresses() {
        return clusterAddresses;
    }

    public List<NodeStateChangeListener> getListeners() {
        return listeners;
    }

    public String getServiceAddress() {
        return serviceAddress;
    }

    public RaftServiceFactory<S> getRaftServiceFactory() {
        return raftServiceFactory;
    }

    public StateMachineFactory<NW, S> getStateMachineFactory() {
        return stateMachineFactory;
    }

    public SnapshotFileOpr<?> getSnapshotFileOpr() {
        return snapshotFileOpr;
    }

    public int getElectionTimeoutMs() {
        return electionTimeoutMs;
    }

    public int getElectionPriority() {
        return electionPriority;
    }

    public int getDecayPriorityGap() {
        return decayPriorityGap;
    }

    public int getLeaderLeaseTimeRatio() {
        return leaderLeaseTimeRatio;
    }

    public int getSnapshotIntervalSecs() {
        return snapshotIntervalSecs;
    }

    public int getSnapshotLogIndexMargin() {
        return snapshotLogIndexMargin;
    }

    public int getCatchupMargin() {
        return catchupMargin;
    }

    public boolean isFilterBeforeCopyRemote() {
        return filterBeforeCopyRemote;
    }

    public boolean isDisableCli() {
        return disableCli;
    }

    public boolean isSharedTimerPool() {
        return sharedTimerPool;
    }

    public int getTimerPoolSize() {
        return timerPoolSize;
    }

    public int getCliRpcThreadPoolSize() {
        return cliRpcThreadPoolSize;
    }

    public int getRaftRpcThreadPoolSize() {
        return raftRpcThreadPoolSize;
    }

    public boolean isEnableMetrics() {
        return enableMetrics;
    }

    public SnapshotThrottle getSnapshotThrottle() {
        return snapshotThrottle;
    }

    public boolean isSharedElectionTimer() {
        return sharedElectionTimer;
    }

    public boolean isSharedVoteTimer() {
        return sharedVoteTimer;
    }

    public boolean isSharedStepDownTimer() {
        return sharedStepDownTimer;
    }

    public boolean isSharedSnapshotTimer() {
        return sharedSnapshotTimer;
    }

    //-------------------------------------------------------builder
    public static <NW extends DefaultStateMachine, S extends RaftService> Builder<NW, S> builder() {
        return new Builder<>();
    }

    /**
     * 仅仅使用raft election功能的builder
     */
    public static <NW extends DefaultStateMachine> Builder<NW, DefaultRaftService> electionBuilder() {
        return new Builder<NW, DefaultRaftService>().raftServiceFactory(RaftServiceFactory.EMPTY);
    }

    /** builder **/
    public static class Builder<NW extends DefaultStateMachine, S extends RaftService> {
        private final RaftServerOptions<NW, S> RaftServerOptions = new RaftServerOptions<>();

        public Builder<NW, S> dataDir(String dataDir) {
            RaftServerOptions.dataDir = dataDir;
            return this;
        }

        public Builder<NW, S> groupId(String groupId) {
            RaftServerOptions.groupId = groupId;
            return this;
        }

        public Builder<NW, S> address(String address) {
            RaftServerOptions.address = address;
            return this;
        }

        public Builder<NW, S> clusterAddresses(String clusterAddresses) {
            RaftServerOptions.clusterAddresses = clusterAddresses;
            return this;
        }

        public Builder<NW, S> listeners(List<NodeStateChangeListener> listeners) {
            RaftServerOptions.listeners.addAll(listeners);
            return this;
        }

        public Builder<NW, S> listeners(NodeStateChangeListener... listeners) {
            return listeners(Arrays.asList(listeners));
        }

        public Builder<NW, S> serviceAddress(String serviceAddress) {
            RaftServerOptions.serviceAddress = serviceAddress;
            return this;
        }

        public Builder<NW, S> raftServiceFactory(RaftServiceFactory<S> factory) {
            RaftServerOptions.raftServiceFactory = factory;
            return this;
        }

        public Builder<NW, S> stateMachineFactory(StateMachineFactory<NW, S> factory) {
            RaftServerOptions.stateMachineFactory = factory;
            return this;
        }

        public Builder<NW, S> snapshotFileOpr(SnapshotFileOpr<?> snapshotFileOpr) {
            RaftServerOptions.snapshotFileOpr = snapshotFileOpr;
            return this;
        }

        public Builder<NW, S> electionTimeoutMs(int electionTimeoutMs) {
            RaftServerOptions.electionTimeoutMs = electionTimeoutMs;
            return this;
        }

        public Builder<NW, S> electionPriority(int electionPriority) {
            RaftServerOptions.electionPriority = electionPriority;
            return this;
        }

        public Builder<NW, S> decayPriorityGap(int decayPriorityGap) {
            RaftServerOptions.decayPriorityGap = decayPriorityGap;
            return this;
        }

        public Builder<NW, S> leaderLeaseTimeRatio(int leaderLeaseTimeRatio) {
            RaftServerOptions.leaderLeaseTimeRatio = leaderLeaseTimeRatio;
            return this;
        }

        public Builder<NW, S> snapshotIntervalSecs(int snapshotIntervalSecs) {
            RaftServerOptions.snapshotIntervalSecs = snapshotIntervalSecs;
            return this;
        }

        public Builder<NW, S> snapshotLogIndexMargin(int snapshotLogIndexMargin) {
            RaftServerOptions.snapshotLogIndexMargin = snapshotLogIndexMargin;
            return this;
        }

        public Builder<NW, S> catchupMargin(int catchupMargin) {
            RaftServerOptions.catchupMargin = catchupMargin;
            return this;
        }

        public Builder<NW, S> filterBeforeCopyRemote() {
            RaftServerOptions.filterBeforeCopyRemote = true;
            return this;
        }

        public Builder<NW, S> disableCli() {
            RaftServerOptions.disableCli = false;
            return this;
        }

        public Builder<NW, S> sharedTimerPool() {
            RaftServerOptions.sharedTimerPool = true;
            return this;
        }

        public Builder<NW, S> timerPoolSize(int timerPoolSize) {
            RaftServerOptions.timerPoolSize = timerPoolSize;
            return this;
        }

        public Builder<NW, S> cliRpcThreadPoolSize(int cliRpcThreadPoolSize) {
            RaftServerOptions.cliRpcThreadPoolSize = cliRpcThreadPoolSize;
            return this;
        }

        public Builder<NW, S> raftRpcThreadPoolSize(int raftRpcThreadPoolSize) {
            RaftServerOptions.raftRpcThreadPoolSize = raftRpcThreadPoolSize;
            return this;
        }

        public Builder<NW, S> enableMetrics() {
            RaftServerOptions.enableMetrics = true;
            return this;
        }

        public Builder<NW, S> snapshotThrottle(SnapshotThrottle snapshotThrottle) {
            RaftServerOptions.snapshotThrottle = snapshotThrottle;
            return this;
        }

        public Builder<NW, S> sharedElectionTimer() {
            RaftServerOptions.sharedElectionTimer = true;
            return this;
        }

        public Builder<NW, S> sharedVoteTimer() {
            RaftServerOptions.sharedVoteTimer = true;
            return this;
        }

        public Builder<NW, S> sharedStepDownTimer() {
            RaftServerOptions.sharedStepDownTimer = true;
            return this;
        }

        public Builder<NW, S> sharedSnapshotTimer() {
            RaftServerOptions.sharedSnapshotTimer = true;
            return this;
        }

        public RaftServerOptions<NW, S> build() {
            return RaftServerOptions;
        }

        public RaftServerBootstrap bootstrap() {
            RaftServerBootstrap bootstrap = new RaftServerBootstrap();
            bootstrap.init(build());
            return bootstrap;
        }
    }
}
