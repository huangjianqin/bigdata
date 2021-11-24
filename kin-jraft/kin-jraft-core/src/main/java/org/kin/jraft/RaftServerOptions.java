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

    /**
     * A follower would become a candidate if it doesn't receive any message
     * from the leader in |election_timeout_ms| milliseconds
     * follower to candidate timeout
     * Default: 1000 (1s)
     */
    private int electionTimeoutMs = 1000;
    /**
     * One node's local priority value would be set to | electionPriority |
     * value when it starts up.If this value is set to 0,the node will never be a leader.
     * If this node doesn't support priority election,then set this value to -1.
     * Default: -1
     */
    private int electionPriority = ElectionPriority.Disabled;
    /**
     * If next leader is not elected until next election timeout, it exponentially
     * decay its local target priority, for example target_priority = target_priority - gap
     * Default: 10
     */
    private int decayPriorityGap = 10;
    /**
     * Leader lease time's ratio of electionTimeoutMs,
     * To minimize the effects of clock drift, we should make that:
     * clockDrift + leaderLeaseTimeoutMs < electionTimeout
     * Default: 90, Max: 100
     */
    private int leaderLeaseTimeRatio = 90;
    /**
     * A snapshot saving would be triggered every |snapshot_interval_s| seconds
     * if this was reset as a positive number
     * If |snapshot_interval_s| <= 0, the time based snapshot would be disabled.
     * Default: 3600 (1 hour)
     */
    private int snapshotIntervalSecs = 3600;
    /**
     * A snapshot saving would be triggered every |snapshot_interval_s| seconds,
     * and at this moment when state machine's lastAppliedIndex value
     * minus lastSnapshotId value is greater than snapshotLogIndexMargin value,
     * the snapshot action will be done really.
     * If |snapshotLogIndexMargin| <= 0, the distance based snapshot would be disable.
     * Default: 0
     */
    private int snapshotLogIndexMargin = 0;
    /**
     * We will regard a adding peer as caught up if the margin between the
     * last_log_index of this peer and the last_log_index of leader is less than
     * |catchup_margin|
     * Default: 1000
     */
    private int catchupMargin = 1000;
    /**
     * If enable, we will filter duplicate files before copy remote snapshot,
     * to avoid useless transmission. Two files in local and remote are duplicate,
     * only if they has the same filename and the same checksum (stored in file meta).
     * Default: false
     */
    private boolean filterBeforeCopyRemote = false;
    /**
     * If non-null, we will pass this throughput_snapshot_throttle to SnapshotExecutor
     * Default: NULL
     * scoped_refptr<SnapshotThrottle>* snapshot_throttle;
     * If true, RPCs through raft_cli will be denied.
     * Default: false
     */
    private boolean disableCli = false;
    /**
     * Whether use global timer pool, if true, the {@code timerPoolSize} will be invalid.
     */
    private boolean sharedTimerPool = false;
    /**
     * Timer manager thread pool size
     */
    private int timerPoolSize = Utils.cpus() * 3 > 20 ? 20 : Utils.cpus() * 3;
    /**
     * CLI service request RPC executor pool size, use default executor if -1.
     */
    private int cliRpcThreadPoolSize = Utils.cpus();
    /**
     * RAFT request RPC executor pool size, use default executor if -1.
     */
    private int raftRpcThreadPoolSize = Utils.cpus() * 6;
    /**
     * Whether to enable metrics for node.
     */
    private boolean enableMetrics = false;
    /**
     * If non-null, we will pass this SnapshotThrottle to SnapshotExecutor
     * Default: NULL
     */
    private SnapshotThrottle snapshotThrottle;
    /**
     * Whether use global election timer
     */
    private boolean sharedElectionTimer = false;
    /**
     * Whether use global vote timer
     */
    private boolean sharedVoteTimer = false;
    /**
     * Whether use global step down timer
     */
    private boolean sharedStepDownTimer = false;
    /**
     * Whether use global snapshot timer
     */
    private boolean sharedSnapshotTimer = false;


    /**
     * Rpc connect timeout in milliseconds
     * Default: 1000(1s)
     */
    private int rpcConnectTimeoutMs = 1000;
    /**
     * RPC request default timeout in milliseconds
     * Default: 5000(5s)
     */
    private int rpcDefaultTimeout = 5000;
    /**
     * Install snapshot RPC request default timeout in milliseconds
     * Default: 5 * 60 * 1000(5min)
     */
    private int rpcInstallSnapshotTimeout = 5 * 60 * 1000;
    /**
     * RPC process thread pool size
     * Default: 80
     */
    private int rpcProcessorThreadPoolSize = 80;
    /**
     * Whether to enable checksum for RPC.
     * Default: false
     */
    private boolean enableRpcChecksum = false;
    /**
     * Metric registry for RPC services, user should not use this field.
     */
    private MetricRegistry metricRegistry;

    /** Maximum of block size per RPC */
    private int maxByteCountPerRpc = 128 * 1024;
    /** File service check hole switch, default disable */
    private boolean fileCheckHole = false;
    /** The maximum number of entries in AppendEntriesRequest */
    private int maxEntriesSize = 1024;
    /** The maximum byte size of AppendEntriesRequest */
    private int maxBodySize = 512 * 1024;
    /** Flush buffer to LogStorage if the buffer size reaches the limit */
    private int maxAppendBufferSize = 256 * 1024;
    /** Maximum election delay time allowed by user */
    private int maxElectionDelayMs = 1000;
    /** Raft election:heartbeat timeout factor */
    private int electionHeartbeatFactor = 10;
    /** Maximum number of tasks that can be applied in a batch */
    private int applyBatch = 32;
    /** Call fsync when need */
    private boolean sync = true;
    /** Sync log meta, snapshot meta and raft meta */
    private boolean syncMeta = false;
    /** Statistics to analyze the performance of db */
    private boolean openStatistics = true;
    /** Whether to enable replicator pipeline. */
    private boolean replicatorPipeline = true;
    /** The maximum replicator pipeline in-flight requests/responses, only valid when enable replicator pipeline. */
    private int maxReplicatorInflightMsgs = 256;
    /** Internal disruptor buffers size for Node/FSMCaller/LogManager etc. */
    private int disruptorBufferSize = 16384;
    /**
     * The maximum timeout in seconds to wait when publishing events into disruptor, default is 10 seconds.
     * If the timeout happens, it may halt the node.
     */
    private int disruptorPublishEventWaitTimeoutSecs = 10;
    /**
     * When true, validate log entry checksum when transferring the log entry from disk or network, default is false.
     * If true, it would hurt the performance of JRAft but gain the data safety.
     *
     * @since 1.2.6
     */
    private boolean enableLogEntryChecksum = false;
    /**
     * ReadOnlyOption specifies how the read only request is processed.
     * <p>
     * {@link ReadOnlyOption#ReadOnlySafe} guarantees the linearizability of the read only request by
     * communicating with the quorum. It is the default and suggested option.
     * <p>
     * {@link ReadOnlyOption#ReadOnlyLeaseBased} ensures linearizability of the read only request by
     * relying on the leader lease. It can be affected by clock drift.
     * If the clock drift is unbounded, leader might keep the lease longer than it
     * should (clock can move backward/pause without any bound). ReadIndex is not safe
     * in that case.
     */
    private ReadOnlyOption readOnlyOptions = ReadOnlyOption.ReadOnlySafe;
    /**
     * Candidate steps down when election reaching timeout, default is true(enabled).
     *
     * @since 1.3.0
     */
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

    public RaftServerOptions<NW, S> setDataDir(String dataDir) {
        this.dataDir = dataDir;
        return this;
    }

    public RaftServerOptions<NW, S> setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public RaftServerOptions<NW, S> setAddress(String address) {
        this.address = address;
        return this;
    }

    public RaftServerOptions<NW, S> setClusterAddresses(String clusterAddresses) {
        this.clusterAddresses = clusterAddresses;
        return this;
    }

    public RaftServerOptions<NW, S> setServiceAddress(String serviceAddress) {
        this.serviceAddress = serviceAddress;
        return this;
    }

    public RaftServerOptions<NW, S> setRaftServiceFactory(RaftServiceFactory<S> raftServiceFactory) {
        this.raftServiceFactory = raftServiceFactory;
        return this;
    }

    public RaftServerOptions<NW, S> setStateMachineFactory(StateMachineFactory<NW, S> stateMachineFactory) {
        this.stateMachineFactory = stateMachineFactory;
        return this;
    }

    public RaftServerOptions<NW, S> setSnapshotFileOpr(SnapshotFileOpr<?> snapshotFileOpr) {
        this.snapshotFileOpr = snapshotFileOpr;
        return this;
    }

    public RaftServerOptions<NW, S> setElectionTimeoutMs(int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
        return this;
    }

    public RaftServerOptions<NW, S> setElectionPriority(int electionPriority) {
        this.electionPriority = electionPriority;
        return this;
    }

    public RaftServerOptions<NW, S> setDecayPriorityGap(int decayPriorityGap) {
        this.decayPriorityGap = decayPriorityGap;
        return this;
    }

    public RaftServerOptions<NW, S> setLeaderLeaseTimeRatio(int leaderLeaseTimeRatio) {
        this.leaderLeaseTimeRatio = leaderLeaseTimeRatio;
        return this;
    }

    public RaftServerOptions<NW, S> setSnapshotIntervalSecs(int snapshotIntervalSecs) {
        this.snapshotIntervalSecs = snapshotIntervalSecs;
        return this;
    }

    public RaftServerOptions<NW, S> setSnapshotLogIndexMargin(int snapshotLogIndexMargin) {
        this.snapshotLogIndexMargin = snapshotLogIndexMargin;
        return this;
    }

    public RaftServerOptions<NW, S> setCatchupMargin(int catchupMargin) {
        this.catchupMargin = catchupMargin;
        return this;
    }

    public RaftServerOptions<NW, S> setFilterBeforeCopyRemote(boolean filterBeforeCopyRemote) {
        this.filterBeforeCopyRemote = filterBeforeCopyRemote;
        return this;
    }

    public RaftServerOptions<NW, S> setDisableCli(boolean disableCli) {
        this.disableCli = disableCli;
        return this;
    }

    public RaftServerOptions<NW, S> setSharedTimerPool(boolean sharedTimerPool) {
        this.sharedTimerPool = sharedTimerPool;
        return this;
    }

    public RaftServerOptions<NW, S> setTimerPoolSize(int timerPoolSize) {
        this.timerPoolSize = timerPoolSize;
        return this;
    }

    public RaftServerOptions<NW, S> setCliRpcThreadPoolSize(int cliRpcThreadPoolSize) {
        this.cliRpcThreadPoolSize = cliRpcThreadPoolSize;
        return this;
    }

    public RaftServerOptions<NW, S> setRaftRpcThreadPoolSize(int raftRpcThreadPoolSize) {
        this.raftRpcThreadPoolSize = raftRpcThreadPoolSize;
        return this;
    }

    public RaftServerOptions<NW, S> setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
        return this;
    }

    public RaftServerOptions<NW, S> setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
        return this;
    }

    public RaftServerOptions<NW, S> setSharedElectionTimer(boolean sharedElectionTimer) {
        this.sharedElectionTimer = sharedElectionTimer;
        return this;
    }

    public RaftServerOptions<NW, S> setSharedVoteTimer(boolean sharedVoteTimer) {
        this.sharedVoteTimer = sharedVoteTimer;
        return this;
    }

    public RaftServerOptions<NW, S> setSharedStepDownTimer(boolean sharedStepDownTimer) {
        this.sharedStepDownTimer = sharedStepDownTimer;
        return this;
    }

    public RaftServerOptions<NW, S> setSharedSnapshotTimer(boolean sharedSnapshotTimer) {
        this.sharedSnapshotTimer = sharedSnapshotTimer;
        return this;
    }

    public RaftServerOptions<NW, S> setRpcConnectTimeoutMs(int rpcConnectTimeoutMs) {
        this.rpcConnectTimeoutMs = rpcConnectTimeoutMs;
        return this;
    }

    public RaftServerOptions<NW, S> setRpcDefaultTimeout(int rpcDefaultTimeout) {
        this.rpcDefaultTimeout = rpcDefaultTimeout;
        return this;
    }

    public RaftServerOptions<NW, S> setRpcInstallSnapshotTimeout(int rpcInstallSnapshotTimeout) {
        this.rpcInstallSnapshotTimeout = rpcInstallSnapshotTimeout;
        return this;
    }

    public RaftServerOptions<NW, S> setRpcProcessorThreadPoolSize(int rpcProcessorThreadPoolSize) {
        this.rpcProcessorThreadPoolSize = rpcProcessorThreadPoolSize;
        return this;
    }

    public RaftServerOptions<NW, S> setEnableRpcChecksum(boolean enableRpcChecksum) {
        this.enableRpcChecksum = enableRpcChecksum;
        return this;
    }

    public RaftServerOptions<NW, S> setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    public RaftServerOptions<NW, S> setMaxByteCountPerRpc(int maxByteCountPerRpc) {
        this.maxByteCountPerRpc = maxByteCountPerRpc;
        return this;
    }

    public RaftServerOptions<NW, S> setFileCheckHole(boolean fileCheckHole) {
        this.fileCheckHole = fileCheckHole;
        return this;
    }

    public RaftServerOptions<NW, S> setMaxEntriesSize(int maxEntriesSize) {
        this.maxEntriesSize = maxEntriesSize;
        return this;
    }

    public RaftServerOptions<NW, S> setMaxBodySize(int maxBodySize) {
        this.maxBodySize = maxBodySize;
        return this;
    }

    public RaftServerOptions<NW, S> setMaxAppendBufferSize(int maxAppendBufferSize) {
        this.maxAppendBufferSize = maxAppendBufferSize;
        return this;
    }

    public RaftServerOptions<NW, S> setMaxElectionDelayMs(int maxElectionDelayMs) {
        this.maxElectionDelayMs = maxElectionDelayMs;
        return this;
    }

    public RaftServerOptions<NW, S> setElectionHeartbeatFactor(int electionHeartbeatFactor) {
        this.electionHeartbeatFactor = electionHeartbeatFactor;
        return this;
    }

    public RaftServerOptions<NW, S> setApplyBatch(int applyBatch) {
        this.applyBatch = applyBatch;
        return this;
    }

    public RaftServerOptions<NW, S> setSync(boolean sync) {
        this.sync = sync;
        return this;
    }

    public RaftServerOptions<NW, S> setSyncMeta(boolean syncMeta) {
        this.syncMeta = syncMeta;
        return this;
    }

    public RaftServerOptions<NW, S> setOpenStatistics(boolean openStatistics) {
        this.openStatistics = openStatistics;
        return this;
    }

    public RaftServerOptions<NW, S> setReplicatorPipeline(boolean replicatorPipeline) {
        this.replicatorPipeline = replicatorPipeline;
        return this;
    }

    public RaftServerOptions<NW, S> setMaxReplicatorInflightMsgs(int maxReplicatorInflightMsgs) {
        this.maxReplicatorInflightMsgs = maxReplicatorInflightMsgs;
        return this;
    }

    public RaftServerOptions<NW, S> setDisruptorBufferSize(int disruptorBufferSize) {
        this.disruptorBufferSize = disruptorBufferSize;
        return this;
    }

    public RaftServerOptions<NW, S> setDisruptorPublishEventWaitTimeoutSecs(int disruptorPublishEventWaitTimeoutSecs) {
        this.disruptorPublishEventWaitTimeoutSecs = disruptorPublishEventWaitTimeoutSecs;
        return this;
    }

    public RaftServerOptions<NW, S> setEnableLogEntryChecksum(boolean enableLogEntryChecksum) {
        this.enableLogEntryChecksum = enableLogEntryChecksum;
        return this;
    }

    public RaftServerOptions<NW, S> setReadOnlyOptions(ReadOnlyOption readOnlyOptions) {
        this.readOnlyOptions = readOnlyOptions;
        return this;
    }

    public RaftServerOptions<NW, S> setStepDownWhenVoteTimedout(boolean stepDownWhenVoteTimedout) {
        this.stepDownWhenVoteTimedout = stepDownWhenVoteTimedout;
        return this;
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
