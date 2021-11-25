package org.kin.jraft.springboot;

import com.alipay.sofa.jraft.core.ElectionPriority;
import com.alipay.sofa.jraft.option.ReadOnlyOption;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.util.Utils;
import com.codahale.metrics.MetricRegistry;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author huangjianqin
 * @date 2021/11/24
 */
@SuppressWarnings("rawtypes")
@ConfigurationProperties("kin.jraft.server")
public class RaftServerProperties {
    /** 数据存储目录 */
    private String dataDir;
    /** raft group id */
    private String groupId;
    /** ip:port */
    private String address;
    /** ip:port,ip:port,ip:port */
    private String clusterAddresses;
    /** ip:port, raft service绑定地址 */
    private String serviceAddress;

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

    //setter && getter
    public String getDataDir() {
        return dataDir;
    }

    public RaftServerProperties setDataDir(String dataDir) {
        this.dataDir = dataDir;
        return this;
    }

    public String getGroupId() {
        return groupId;
    }

    public RaftServerProperties setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public String getAddress() {
        return address;
    }

    public RaftServerProperties setAddress(String address) {
        this.address = address;
        return this;
    }

    public String getClusterAddresses() {
        return clusterAddresses;
    }

    public RaftServerProperties setClusterAddresses(String clusterAddresses) {
        this.clusterAddresses = clusterAddresses;
        return this;
    }

    public String getServiceAddress() {
        return serviceAddress;
    }

    public RaftServerProperties setServiceAddress(String serviceAddress) {
        this.serviceAddress = serviceAddress;
        return this;
    }

    public int getElectionTimeoutMs() {
        return electionTimeoutMs;
    }

    public RaftServerProperties setElectionTimeoutMs(int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
        return this;
    }

    public int getElectionPriority() {
        return electionPriority;
    }

    public RaftServerProperties setElectionPriority(int electionPriority) {
        this.electionPriority = electionPriority;
        return this;
    }

    public int getDecayPriorityGap() {
        return decayPriorityGap;
    }

    public RaftServerProperties setDecayPriorityGap(int decayPriorityGap) {
        this.decayPriorityGap = decayPriorityGap;
        return this;
    }

    public int getLeaderLeaseTimeRatio() {
        return leaderLeaseTimeRatio;
    }

    public RaftServerProperties setLeaderLeaseTimeRatio(int leaderLeaseTimeRatio) {
        this.leaderLeaseTimeRatio = leaderLeaseTimeRatio;
        return this;
    }

    public int getSnapshotIntervalSecs() {
        return snapshotIntervalSecs;
    }

    public RaftServerProperties setSnapshotIntervalSecs(int snapshotIntervalSecs) {
        this.snapshotIntervalSecs = snapshotIntervalSecs;
        return this;
    }

    public int getSnapshotLogIndexMargin() {
        return snapshotLogIndexMargin;
    }

    public RaftServerProperties setSnapshotLogIndexMargin(int snapshotLogIndexMargin) {
        this.snapshotLogIndexMargin = snapshotLogIndexMargin;
        return this;
    }

    public int getCatchupMargin() {
        return catchupMargin;
    }

    public RaftServerProperties setCatchupMargin(int catchupMargin) {
        this.catchupMargin = catchupMargin;
        return this;
    }

    public boolean isFilterBeforeCopyRemote() {
        return filterBeforeCopyRemote;
    }

    public RaftServerProperties setFilterBeforeCopyRemote(boolean filterBeforeCopyRemote) {
        this.filterBeforeCopyRemote = filterBeforeCopyRemote;
        return this;
    }

    public boolean isDisableCli() {
        return disableCli;
    }

    public RaftServerProperties setDisableCli(boolean disableCli) {
        this.disableCli = disableCli;
        return this;
    }

    public boolean isSharedTimerPool() {
        return sharedTimerPool;
    }

    public RaftServerProperties setSharedTimerPool(boolean sharedTimerPool) {
        this.sharedTimerPool = sharedTimerPool;
        return this;
    }

    public int getTimerPoolSize() {
        return timerPoolSize;
    }

    public RaftServerProperties setTimerPoolSize(int timerPoolSize) {
        this.timerPoolSize = timerPoolSize;
        return this;
    }

    public int getCliRpcThreadPoolSize() {
        return cliRpcThreadPoolSize;
    }

    public RaftServerProperties setCliRpcThreadPoolSize(int cliRpcThreadPoolSize) {
        this.cliRpcThreadPoolSize = cliRpcThreadPoolSize;
        return this;
    }

    public int getRaftRpcThreadPoolSize() {
        return raftRpcThreadPoolSize;
    }

    public RaftServerProperties setRaftRpcThreadPoolSize(int raftRpcThreadPoolSize) {
        this.raftRpcThreadPoolSize = raftRpcThreadPoolSize;
        return this;
    }

    public boolean isEnableMetrics() {
        return enableMetrics;
    }

    public RaftServerProperties setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
        return this;
    }

    public SnapshotThrottle getSnapshotThrottle() {
        return snapshotThrottle;
    }

    public RaftServerProperties setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
        return this;
    }

    public boolean isSharedElectionTimer() {
        return sharedElectionTimer;
    }

    public RaftServerProperties setSharedElectionTimer(boolean sharedElectionTimer) {
        this.sharedElectionTimer = sharedElectionTimer;
        return this;
    }

    public boolean isSharedVoteTimer() {
        return sharedVoteTimer;
    }

    public RaftServerProperties setSharedVoteTimer(boolean sharedVoteTimer) {
        this.sharedVoteTimer = sharedVoteTimer;
        return this;
    }

    public boolean isSharedStepDownTimer() {
        return sharedStepDownTimer;
    }

    public RaftServerProperties setSharedStepDownTimer(boolean sharedStepDownTimer) {
        this.sharedStepDownTimer = sharedStepDownTimer;
        return this;
    }

    public boolean isSharedSnapshotTimer() {
        return sharedSnapshotTimer;
    }

    public RaftServerProperties setSharedSnapshotTimer(boolean sharedSnapshotTimer) {
        this.sharedSnapshotTimer = sharedSnapshotTimer;
        return this;
    }

    public int getRpcConnectTimeoutMs() {
        return rpcConnectTimeoutMs;
    }

    public RaftServerProperties setRpcConnectTimeoutMs(int rpcConnectTimeoutMs) {
        this.rpcConnectTimeoutMs = rpcConnectTimeoutMs;
        return this;
    }

    public int getRpcDefaultTimeout() {
        return rpcDefaultTimeout;
    }

    public RaftServerProperties setRpcDefaultTimeout(int rpcDefaultTimeout) {
        this.rpcDefaultTimeout = rpcDefaultTimeout;
        return this;
    }

    public int getRpcInstallSnapshotTimeout() {
        return rpcInstallSnapshotTimeout;
    }

    public RaftServerProperties setRpcInstallSnapshotTimeout(int rpcInstallSnapshotTimeout) {
        this.rpcInstallSnapshotTimeout = rpcInstallSnapshotTimeout;
        return this;
    }

    public int getRpcProcessorThreadPoolSize() {
        return rpcProcessorThreadPoolSize;
    }

    public RaftServerProperties setRpcProcessorThreadPoolSize(int rpcProcessorThreadPoolSize) {
        this.rpcProcessorThreadPoolSize = rpcProcessorThreadPoolSize;
        return this;
    }

    public boolean isEnableRpcChecksum() {
        return enableRpcChecksum;
    }

    public RaftServerProperties setEnableRpcChecksum(boolean enableRpcChecksum) {
        this.enableRpcChecksum = enableRpcChecksum;
        return this;
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public RaftServerProperties setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    public int getMaxByteCountPerRpc() {
        return maxByteCountPerRpc;
    }

    public RaftServerProperties setMaxByteCountPerRpc(int maxByteCountPerRpc) {
        this.maxByteCountPerRpc = maxByteCountPerRpc;
        return this;
    }

    public boolean isFileCheckHole() {
        return fileCheckHole;
    }

    public RaftServerProperties setFileCheckHole(boolean fileCheckHole) {
        this.fileCheckHole = fileCheckHole;
        return this;
    }

    public int getMaxEntriesSize() {
        return maxEntriesSize;
    }

    public RaftServerProperties setMaxEntriesSize(int maxEntriesSize) {
        this.maxEntriesSize = maxEntriesSize;
        return this;
    }

    public int getMaxBodySize() {
        return maxBodySize;
    }

    public RaftServerProperties setMaxBodySize(int maxBodySize) {
        this.maxBodySize = maxBodySize;
        return this;
    }

    public int getMaxAppendBufferSize() {
        return maxAppendBufferSize;
    }

    public RaftServerProperties setMaxAppendBufferSize(int maxAppendBufferSize) {
        this.maxAppendBufferSize = maxAppendBufferSize;
        return this;
    }

    public int getMaxElectionDelayMs() {
        return maxElectionDelayMs;
    }

    public RaftServerProperties setMaxElectionDelayMs(int maxElectionDelayMs) {
        this.maxElectionDelayMs = maxElectionDelayMs;
        return this;
    }

    public int getElectionHeartbeatFactor() {
        return electionHeartbeatFactor;
    }

    public RaftServerProperties setElectionHeartbeatFactor(int electionHeartbeatFactor) {
        this.electionHeartbeatFactor = electionHeartbeatFactor;
        return this;
    }

    public int getApplyBatch() {
        return applyBatch;
    }

    public RaftServerProperties setApplyBatch(int applyBatch) {
        this.applyBatch = applyBatch;
        return this;
    }

    public boolean isSync() {
        return sync;
    }

    public RaftServerProperties setSync(boolean sync) {
        this.sync = sync;
        return this;
    }

    public boolean isSyncMeta() {
        return syncMeta;
    }

    public RaftServerProperties setSyncMeta(boolean syncMeta) {
        this.syncMeta = syncMeta;
        return this;
    }

    public boolean isOpenStatistics() {
        return openStatistics;
    }

    public RaftServerProperties setOpenStatistics(boolean openStatistics) {
        this.openStatistics = openStatistics;
        return this;
    }

    public boolean isReplicatorPipeline() {
        return replicatorPipeline;
    }

    public RaftServerProperties setReplicatorPipeline(boolean replicatorPipeline) {
        this.replicatorPipeline = replicatorPipeline;
        return this;
    }

    public int getMaxReplicatorInflightMsgs() {
        return maxReplicatorInflightMsgs;
    }

    public RaftServerProperties setMaxReplicatorInflightMsgs(int maxReplicatorInflightMsgs) {
        this.maxReplicatorInflightMsgs = maxReplicatorInflightMsgs;
        return this;
    }

    public int getDisruptorBufferSize() {
        return disruptorBufferSize;
    }

    public RaftServerProperties setDisruptorBufferSize(int disruptorBufferSize) {
        this.disruptorBufferSize = disruptorBufferSize;
        return this;
    }

    public int getDisruptorPublishEventWaitTimeoutSecs() {
        return disruptorPublishEventWaitTimeoutSecs;
    }

    public RaftServerProperties setDisruptorPublishEventWaitTimeoutSecs(int disruptorPublishEventWaitTimeoutSecs) {
        this.disruptorPublishEventWaitTimeoutSecs = disruptorPublishEventWaitTimeoutSecs;
        return this;
    }

    public boolean isEnableLogEntryChecksum() {
        return enableLogEntryChecksum;
    }

    public RaftServerProperties setEnableLogEntryChecksum(boolean enableLogEntryChecksum) {
        this.enableLogEntryChecksum = enableLogEntryChecksum;
        return this;
    }

    public ReadOnlyOption getReadOnlyOptions() {
        return readOnlyOptions;
    }

    public RaftServerProperties setReadOnlyOptions(ReadOnlyOption readOnlyOptions) {
        this.readOnlyOptions = readOnlyOptions;
        return this;
    }

    public boolean isStepDownWhenVoteTimedout() {
        return stepDownWhenVoteTimedout;
    }

    public RaftServerProperties setStepDownWhenVoteTimedout(boolean stepDownWhenVoteTimedout) {
        this.stepDownWhenVoteTimedout = stepDownWhenVoteTimedout;
        return this;
    }
}
