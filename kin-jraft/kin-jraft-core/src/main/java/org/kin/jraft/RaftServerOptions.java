package org.kin.jraft;

import com.alipay.sofa.jraft.core.ElectionPriority;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReadOnlyOption;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.util.Utils;
import com.codahale.metrics.MetricRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * raft选举配置
 *
 * @author huangjianqin
 * @date 2021/11/7
 */
public final class RaftServerOptions<NW extends DefaultStateMachine<?>, S extends RaftService> {
    /** 数据存储目录 */
    private String dataDir;
    /** raft group id */
    private String groupId;
    /** ip:port */
    private String address;
    /** ip:port,ip:port,ip:port */
    private String clusterAddresses;
    /** {@link NodeStateChangeListener} list */
    private final List<NodeStateChangeListener> listeners = new ArrayList<>();
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
    /** {@link NodeOptions#getRpcConnectTimeoutMs()} */
    private int rpcConnectTimeoutMs = 1000;

    /** {@link NodeOptions#getRpcConnectTimeoutMs()} */
    private int rpcDefaultTimeout = 5000;
    /** {@link NodeOptions#getRpcInstallSnapshotTimeout()} */
    private int rpcInstallSnapshotTimeout = 5 * 60 * 1000;
    /** {@link NodeOptions#getRpcProcessorThreadPoolSize()} */
    private int rpcProcessorThreadPoolSize = 80;
    /** {@link NodeOptions#isEnableRpcChecksum()} */
    private boolean enableRpcChecksum = false;
    /** {@link NodeOptions#getMetricRegistry()} */
    private MetricRegistry metricRegistry;

    /** {@link RaftOptions#getMaxByteCountPerRpc()} */
    private int maxByteCountPerRpc = 128 * 1024;
    /** {@link RaftOptions#isFileCheckHole()} */
    private boolean fileCheckHole = false;
    /** {@link RaftOptions#getMaxEntriesSize()} */
    private int maxEntriesSize = 1024;
    /** {@link RaftOptions#getMaxBodySize()} */
    private int maxBodySize = 512 * 1024;
    /** {@link RaftOptions#getMaxAppendBufferSize()} */
    private int maxAppendBufferSize = 256 * 1024;
    /** {@link RaftOptions#getMaxElectionDelayMs()} */
    private int maxElectionDelayMs = 1000;
    /** {@link RaftOptions#getElectionHeartbeatFactor()} */
    private int electionHeartbeatFactor = 10;
    /** {@link RaftOptions#getApplyBatch()} */
    private int applyBatch = 32;
    /** {@link RaftOptions#isSync()} */
    private boolean sync = true;
    /** {@link RaftOptions#isSyncMeta()} */
    private boolean syncMeta = false;
    /** {@link RaftOptions#isOpenStatistics()} */
    private boolean openStatistics = true;
    /** {@link RaftOptions#isReplicatorPipeline()} */
    private boolean replicatorPipeline = true;
    /** {@link RaftOptions#getMaxReplicatorInflightMsgs()} */
    private int maxReplicatorInflightMsgs = 256;
    /** {@link RaftOptions#getDisruptorBufferSize()} ()} */
    private int disruptorBufferSize = 16384;
    /** {@link RaftOptions#getDisruptorPublishEventWaitTimeoutSecs()} */
    private int disruptorPublishEventWaitTimeoutSecs = 10;
    /** {@link RaftOptions#isEnableLogEntryChecksum()} */
    private boolean enableLogEntryChecksum = false;
    /** {@link RaftOptions#getReadOnlyOptions()} */
    private ReadOnlyOption readOnlyOptions = ReadOnlyOption.ReadOnlySafe;
    /** {@link RaftOptions#isStepDownWhenVoteTimedout()} */
    private boolean stepDownWhenVoteTimedout = true;

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

        nodeOpts.setRpcDefaultTimeout(rpcDefaultTimeout);
        nodeOpts.setRpcInstallSnapshotTimeout(rpcInstallSnapshotTimeout);
        nodeOpts.setRpcProcessorThreadPoolSize(rpcProcessorThreadPoolSize);
        nodeOpts.setEnableRpcChecksum(enableRpcChecksum);
        nodeOpts.setMetricRegistry(metricRegistry);

        RaftOptions raftOptions = new RaftOptions();
        setupOtherRaftOptions(raftOptions);
        nodeOpts.setRaftOptions(raftOptions);
    }

    void setupOtherRaftOptions(RaftOptions raftOptions) {
        raftOptions.setMaxByteCountPerRpc(maxByteCountPerRpc);
        raftOptions.setFileCheckHole(fileCheckHole);
        raftOptions.setMaxEntriesSize(maxEntriesSize);
        raftOptions.setMaxBodySize(maxBodySize);
        raftOptions.setMaxAppendBufferSize(maxAppendBufferSize);
        raftOptions.setMaxElectionDelayMs(maxElectionDelayMs);
        raftOptions.setElectionHeartbeatFactor(electionHeartbeatFactor);
        raftOptions.setApplyBatch(applyBatch);
        raftOptions.setSync(sync);
        raftOptions.setSyncMeta(syncMeta);
        raftOptions.setOpenStatistics(openStatistics);
        raftOptions.setReplicatorPipeline(replicatorPipeline);
        raftOptions.setMaxReplicatorInflightMsgs(maxReplicatorInflightMsgs);
        raftOptions.setDisruptorBufferSize(disruptorBufferSize);
        raftOptions.setDisruptorPublishEventWaitTimeoutSecs(disruptorPublishEventWaitTimeoutSecs);
        raftOptions.setEnableLogEntryChecksum(enableLogEntryChecksum);
        raftOptions.setReadOnlyOptions(readOnlyOptions);
        raftOptions.setStepDownWhenVoteTimedout(stepDownWhenVoteTimedout);
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

    public int getRpcConnectTimeoutMs() {
        return rpcConnectTimeoutMs;
    }

    public int getRpcDefaultTimeout() {
        return rpcDefaultTimeout;
    }

    public int getRpcInstallSnapshotTimeout() {
        return rpcInstallSnapshotTimeout;
    }

    public int getRpcProcessorThreadPoolSize() {
        return rpcProcessorThreadPoolSize;
    }

    public boolean isEnableRpcChecksum() {
        return enableRpcChecksum;
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public int getMaxByteCountPerRpc() {
        return maxByteCountPerRpc;
    }

    public boolean isFileCheckHole() {
        return fileCheckHole;
    }

    public int getMaxEntriesSize() {
        return maxEntriesSize;
    }

    public int getMaxBodySize() {
        return maxBodySize;
    }

    public int getMaxAppendBufferSize() {
        return maxAppendBufferSize;
    }

    public int getMaxElectionDelayMs() {
        return maxElectionDelayMs;
    }

    public int getElectionHeartbeatFactor() {
        return electionHeartbeatFactor;
    }

    public int getApplyBatch() {
        return applyBatch;
    }

    public boolean isSync() {
        return sync;
    }

    public boolean isSyncMeta() {
        return syncMeta;
    }

    public boolean isOpenStatistics() {
        return openStatistics;
    }

    public boolean isReplicatorPipeline() {
        return replicatorPipeline;
    }

    public int getMaxReplicatorInflightMsgs() {
        return maxReplicatorInflightMsgs;
    }

    public int getDisruptorBufferSize() {
        return disruptorBufferSize;
    }

    public int getDisruptorPublishEventWaitTimeoutSecs() {
        return disruptorPublishEventWaitTimeoutSecs;
    }

    public boolean isEnableLogEntryChecksum() {
        return enableLogEntryChecksum;
    }

    public ReadOnlyOption getReadOnlyOptions() {
        return readOnlyOptions;
    }

    public boolean isStepDownWhenVoteTimedout() {
        return stepDownWhenVoteTimedout;
    }

    //-------------------------------------------------------builder
    public static <NW extends DefaultStateMachine<?>, S extends RaftService> Builder<NW, S> builder() {
        return new Builder<>();
    }

    /**
     * 仅仅使用raft election功能的builder
     */
    public static <NW extends DefaultStateMachine<?>> Builder<NW, DefaultRaftService> electionBuilder() {
        return new Builder<NW, DefaultRaftService>().raftServiceFactory(RaftServiceFactory.EMPTY);
    }

    /** builder **/
    public static class Builder<NW extends DefaultStateMachine<?>, S extends RaftService> {
        private final RaftServerOptions<NW, S> raftServerOptions = new RaftServerOptions<>();

        public Builder<NW, S> dataDir(String dataDir) {
            raftServerOptions.dataDir = dataDir;
            return this;
        }

        public Builder<NW, S> groupId(String groupId) {
            raftServerOptions.groupId = groupId;
            return this;
        }

        public Builder<NW, S> address(String address) {
            raftServerOptions.address = address;
            return this;
        }

        public Builder<NW, S> clusterAddresses(String clusterAddresses) {
            raftServerOptions.clusterAddresses = clusterAddresses;
            return this;
        }

        public Builder<NW, S> listeners(List<NodeStateChangeListener> listeners) {
            raftServerOptions.listeners.addAll(listeners);
            return this;
        }

        public Builder<NW, S> listeners(NodeStateChangeListener... listeners) {
            return listeners(Arrays.asList(listeners));
        }

        public Builder<NW, S> serviceAddress(String serviceAddress) {
            raftServerOptions.serviceAddress = serviceAddress;
            return this;
        }

        public Builder<NW, S> raftServiceFactory(RaftServiceFactory<S> factory) {
            raftServerOptions.raftServiceFactory = factory;
            return this;
        }

        public Builder<NW, S> stateMachineFactory(StateMachineFactory<NW, S> factory) {
            raftServerOptions.stateMachineFactory = factory;
            return this;
        }

        public Builder<NW, S> snapshotFileOpr(SnapshotFileOpr<?> snapshotFileOpr) {
            raftServerOptions.snapshotFileOpr = snapshotFileOpr;
            return this;
        }

        public Builder<NW, S> electionTimeoutMs(int electionTimeoutMs) {
            raftServerOptions.electionTimeoutMs = electionTimeoutMs;
            return this;
        }

        public Builder<NW, S> electionPriority(int electionPriority) {
            raftServerOptions.electionPriority = electionPriority;
            return this;
        }

        public Builder<NW, S> decayPriorityGap(int decayPriorityGap) {
            raftServerOptions.decayPriorityGap = decayPriorityGap;
            return this;
        }

        public Builder<NW, S> leaderLeaseTimeRatio(int leaderLeaseTimeRatio) {
            raftServerOptions.leaderLeaseTimeRatio = leaderLeaseTimeRatio;
            return this;
        }

        public Builder<NW, S> snapshotIntervalSecs(int snapshotIntervalSecs) {
            raftServerOptions.snapshotIntervalSecs = snapshotIntervalSecs;
            return this;
        }

        public Builder<NW, S> snapshotLogIndexMargin(int snapshotLogIndexMargin) {
            raftServerOptions.snapshotLogIndexMargin = snapshotLogIndexMargin;
            return this;
        }

        public Builder<NW, S> catchupMargin(int catchupMargin) {
            raftServerOptions.catchupMargin = catchupMargin;
            return this;
        }

        public Builder<NW, S> filterBeforeCopyRemote() {
            raftServerOptions.filterBeforeCopyRemote = true;
            return this;
        }

        public Builder<NW, S> disableCli() {
            raftServerOptions.disableCli = false;
            return this;
        }

        public Builder<NW, S> sharedTimerPool() {
            raftServerOptions.sharedTimerPool = true;
            return this;
        }

        public Builder<NW, S> timerPoolSize(int timerPoolSize) {
            raftServerOptions.timerPoolSize = timerPoolSize;
            return this;
        }

        public Builder<NW, S> cliRpcThreadPoolSize(int cliRpcThreadPoolSize) {
            raftServerOptions.cliRpcThreadPoolSize = cliRpcThreadPoolSize;
            return this;
        }

        public Builder<NW, S> raftRpcThreadPoolSize(int raftRpcThreadPoolSize) {
            raftServerOptions.raftRpcThreadPoolSize = raftRpcThreadPoolSize;
            return this;
        }

        public Builder<NW, S> enableMetrics() {
            raftServerOptions.enableMetrics = true;
            return this;
        }

        public Builder<NW, S> snapshotThrottle(SnapshotThrottle snapshotThrottle) {
            raftServerOptions.snapshotThrottle = snapshotThrottle;
            return this;
        }

        public Builder<NW, S> sharedElectionTimer() {
            raftServerOptions.sharedElectionTimer = true;
            return this;
        }

        public Builder<NW, S> sharedVoteTimer() {
            raftServerOptions.sharedVoteTimer = true;
            return this;
        }

        public Builder<NW, S> sharedStepDownTimer() {
            raftServerOptions.sharedStepDownTimer = true;
            return this;
        }

        public Builder<NW, S> sharedSnapshotTimer() {
            raftServerOptions.sharedSnapshotTimer = true;
            return this;
        }

        public Builder<NW, S> rpcConnectTimeoutMs(int rpcConnectTimeoutMs) {
            raftServerOptions.rpcConnectTimeoutMs = rpcConnectTimeoutMs;
            return this;
        }

        public Builder<NW, S> rpcDefaultTimeout(int rpcDefaultTimeout) {
            raftServerOptions.rpcDefaultTimeout = rpcDefaultTimeout;
            return this;
        }

        public Builder<NW, S> rpcInstallSnapshotTimeout(int rpcInstallSnapshotTimeout) {
            raftServerOptions.rpcInstallSnapshotTimeout = rpcInstallSnapshotTimeout;
            return this;
        }

        public Builder<NW, S> rpcProcessorThreadPoolSize(int rpcProcessorThreadPoolSize) {
            raftServerOptions.rpcProcessorThreadPoolSize = rpcProcessorThreadPoolSize;
            return this;
        }

        public Builder<NW, S> enableRpcChecksum() {
            raftServerOptions.enableRpcChecksum = true;
            return this;
        }

        public Builder<NW, S> metricRegistry(MetricRegistry metricRegistry) {
            raftServerOptions.metricRegistry = metricRegistry;
            return this;
        }

        public Builder<NW, S> maxByteCountPerRpc(int maxByteCountPerRpc) {
            raftServerOptions.maxByteCountPerRpc = maxByteCountPerRpc;
            return this;
        }

        public Builder<NW, S> fileCheckHole() {
            raftServerOptions.fileCheckHole = true;
            return this;
        }

        public Builder<NW, S> maxEntriesSize(int maxEntriesSize) {
            raftServerOptions.maxEntriesSize = maxEntriesSize;
            return this;
        }

        public Builder<NW, S> maxBodySize(int maxBodySize) {
            raftServerOptions.maxBodySize = maxBodySize;
            return this;
        }

        public Builder<NW, S> maxAppendBufferSize(int maxAppendBufferSize) {
            raftServerOptions.maxAppendBufferSize = maxAppendBufferSize;
            return this;
        }

        public Builder<NW, S> maxElectionDelayMs(int maxElectionDelayMs) {
            raftServerOptions.maxElectionDelayMs = maxElectionDelayMs;
            return this;
        }

        public Builder<NW, S> electionHeartbeatFactor(int electionHeartbeatFactor) {
            raftServerOptions.electionHeartbeatFactor = electionHeartbeatFactor;
            return this;
        }

        public Builder<NW, S> applyBatch(int applyBatch) {
            raftServerOptions.applyBatch = applyBatch;
            return this;
        }

        public Builder<NW, S> sync() {
            raftServerOptions.sync = true;
            return this;
        }

        public Builder<NW, S> syncMeta() {
            raftServerOptions.syncMeta = true;
            return this;
        }

        public Builder<NW, S> openStatistics() {
            raftServerOptions.openStatistics = true;
            return this;
        }

        public Builder<NW, S> replicatorPipeline() {
            raftServerOptions.replicatorPipeline = true;
            return this;
        }

        public Builder<NW, S> maxReplicatorInflightMsgs(int maxReplicatorInflightMsgs) {
            raftServerOptions.maxReplicatorInflightMsgs = maxReplicatorInflightMsgs;
            return this;
        }

        public Builder<NW, S> disruptorBufferSize(int disruptorBufferSize) {
            raftServerOptions.disruptorBufferSize = disruptorBufferSize;
            return this;
        }

        public Builder<NW, S> disruptorPublishEventWaitTimeoutSecs(int disruptorPublishEventWaitTimeoutSecs) {
            raftServerOptions.disruptorPublishEventWaitTimeoutSecs = disruptorPublishEventWaitTimeoutSecs;
            return this;
        }

        public Builder<NW, S> enableLogEntryChecksum() {
            raftServerOptions.enableLogEntryChecksum = true;
            return this;
        }

        public Builder<NW, S> readOnlyOptions(ReadOnlyOption readOnlyOptions) {
            raftServerOptions.readOnlyOptions = readOnlyOptions;
            return this;
        }

        public Builder<NW, S> stepDownWhenVoteTimedout() {
            raftServerOptions.stepDownWhenVoteTimedout = true;
            return this;
        }

        public RaftServerOptions<NW, S> build() {
            return raftServerOptions;
        }

        public RaftServerBootstrap bootstrap() {
            RaftServerBootstrap bootstrap = new RaftServerBootstrap();
            bootstrap.init(build());
            return bootstrap;
        }
    }
}
