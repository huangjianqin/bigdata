package org.kin.jraft;

import com.alipay.sofa.jraft.option.CliOptions;

/**
 * @author huangjianqin
 * @date 2021/11/7
 */
public class RaftClientOptions {
    /** raft group id */
    private String groupId;
    /** ip:port,ip:port,ip:port */
    private String clusterAddresses;
    /** ip:port, raft service绑定地址 */
    private String serviceAddress;
    /** rpc call默认超时时间, {@link CliOptions#getTimeoutMs()} */
    private int timeoutMs;
    /** rpc call最大尝试次数, {@link CliOptions#getMaxRetry()} */
    private int maxRetry;

    CliOptions getCliOptions() {
        CliOptions cliOptions = new CliOptions();
        cliOptions.setTimeoutMs(timeoutMs);
        cliOptions.setMaxRetry(maxRetry);
        return cliOptions;
    }

    //getter
    public String getGroupId() {
        return groupId;
    }

    public String getClusterAddresses() {
        return clusterAddresses;
    }

    public String getServiceAddress() {
        return serviceAddress;
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public RaftClientOptions setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public RaftClientOptions setClusterAddresses(String clusterAddresses) {
        this.clusterAddresses = clusterAddresses;
        return this;
    }

    public RaftClientOptions setServiceAddress(String serviceAddress) {
        this.serviceAddress = serviceAddress;
        return this;
    }

    public RaftClientOptions setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public RaftClientOptions setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
        return this;
    }

    //-------------------------------------------------------builder
    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final RaftClientOptions raftClientOptions = new RaftClientOptions();

        public Builder groupId(String groupId) {
            raftClientOptions.groupId = groupId;
            return this;
        }

        public Builder clusterAddresses(String clusterAddresses) {
            raftClientOptions.clusterAddresses = clusterAddresses;
            return this;
        }

        public Builder serviceAddress(String serviceAddress) {
            raftClientOptions.serviceAddress = serviceAddress;
            return this;
        }

        public Builder timeoutMs(int timeoutMs) {
            raftClientOptions.timeoutMs = timeoutMs;
            return this;
        }

        public Builder maxRetry(int maxRetry) {
            raftClientOptions.maxRetry = maxRetry;
            return this;
        }

        public RaftClientOptions build() {
            return raftClientOptions;
        }

        public RaftClient connect() {
            RaftClientOptions clientOptions = build();
            RaftClient raftClient = new RaftClient();
            raftClient.init(clientOptions);
            return raftClient;
        }
    }
}
