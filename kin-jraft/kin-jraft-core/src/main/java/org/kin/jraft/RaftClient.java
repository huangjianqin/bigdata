package org.kin.jraft;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.util.Endpoint;
import org.kin.framework.Closeable;
import org.kin.framework.utils.ExceptionUtils;
import org.kin.framework.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2021/11/7
 */
public final class RaftClient implements Lifecycle<RaftClientOptions>, Closeable {
    private static final Logger log = LoggerFactory.getLogger(RaftClient.class);

    private RaftClientOptions opts;
    /** raft client service */
    private CliClientServiceImpl clientService;
    /** raft service endpoint */
    @Nullable
    private Endpoint serviceEndpoint;

    @Override
    public synchronized boolean init(RaftClientOptions opts) {
        if (Objects.isNull(opts)) {
            log.info("RaftClient already started");
            return false;
        }
        this.opts = opts;
        String groupId = opts.getGroupId();
        String clusterAddresses = opts.getClusterAddresses();
        String serviceAddress = opts.getServiceAddress();

        if (StringUtils.isNotBlank(serviceAddress)) {
            PeerId servicePeerId = new PeerId();
            if (!servicePeerId.parse(serviceAddress)) {
                throw new IllegalArgumentException("fail to parse servicePeerId: " + serviceAddress);
            }
            serviceEndpoint = servicePeerId.getEndpoint();
        }

        Configuration conf = new Configuration();
        if (!conf.parse(clusterAddresses)) {
            throw new IllegalArgumentException("fail to parse conf:" + clusterAddresses);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        clientService = new CliClientServiceImpl();
        clientService.init(opts.getCliOptions());

        try {
            if (!RouteTable.getInstance().refreshLeader(clientService, groupId, 1000).isOk()) {
                throw new IllegalStateException("refresh leader failed");
            }
        } catch (Exception e) {
            ExceptionUtils.throwExt(e);
        }
        return true;
    }

    @Override
    public void shutdown() {
        if (Objects.isNull(opts)) {
            return;
        }
        clientService.shutdown();
    }

    @Override
    public void close() {
        shutdown();
    }

    private void checkState() {
        if (Objects.isNull(opts)) {
            throw new IllegalStateException("RaftClient is not started");
        }
    }

    /**
     * 获取raft node leader endpoint
     */
    private Endpoint getLeaderEndpoint() {
        return getLeader().getEndpoint();
    }

    /**
     * 往raft leader异步请求消息
     */
    public void invokeLeaderAsync(Object request, InvokeCallback callback, long timeoutMs) {
        checkState();
        try {
            clientService.getRpcClient().invokeAsync(getLeaderEndpoint(), request, callback, timeoutMs);
        } catch (Exception e) {
            ExceptionUtils.throwExt(e);
        }
    }

    /**
     * 往raft leader同步请求消息
     */
    @SuppressWarnings("unchecked")
    public <T> T invokeLeaderSync(Object request, long timeoutMs) {
        checkState();
        try {
            return (T) clientService.getRpcClient().invokeSync(getLeaderEndpoint(), request, timeoutMs);
        } catch (Exception e) {
            ExceptionUtils.throwExt(e);
        }

        return null;
    }

    /**
     * 往raft service异步请求消息
     */
    public void invokeRaftServiceAsync(Object request, InvokeCallback callback, long timeoutMs) {
        checkState();
        if (Objects.isNull(serviceEndpoint)) {
            //如果没有配置raft service address, 则直接往leader请求, 这里认为raft service与raft node使用同一rpc server
            invokeLeaderAsync(request, callback, timeoutMs);
        }

        try {
            clientService.getRpcClient().invokeAsync(serviceEndpoint, request, callback, timeoutMs);
        } catch (Exception e) {
            ExceptionUtils.throwExt(e);
        }
    }

    /**
     * 往raft service同步请求消息
     */
    @SuppressWarnings("unchecked")
    public <T> T invokeRaftServiceSync(Object request, long timeoutMs) {
        checkState();
        if (Objects.isNull(serviceEndpoint)) {
            //如果没有配置raft service address, 则直接往leader请求, 这里认为raft service与raft node使用同一rpc server
            return invokeLeaderSync(request, timeoutMs);
        }

        try {
            return (T) clientService.getRpcClient().invokeSync(serviceEndpoint, request, timeoutMs);
        } catch (Exception e) {
            ExceptionUtils.throwExt(e);
        }

        return null;
    }

    //getter
    public RaftClientOptions getOpts() {
        return opts;
    }

    public PeerId getLeader() {
        return RouteTable.getInstance().selectLeader(opts.getGroupId());
    }
}
