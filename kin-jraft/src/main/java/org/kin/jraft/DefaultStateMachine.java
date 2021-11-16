package org.kin.jraft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import org.kin.framework.log.LoggerOprs;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 监听当前节点状态变化的{@link com.alipay.sofa.jraft.StateMachine}实现
 *
 * @param <T> 状态机value
 * @author huangjianqin
 * @date 2021/11/7
 */
public class DefaultStateMachine<T> extends StateMachineAdapter implements LoggerOprs {
    /** leader任期号 */
    private final AtomicLong leaderTerm = new AtomicLong(-1L);
    /** 注册的{@link NodeStateChangeListener}实例 */
    private final List<NodeStateChangeListener> listeners = new CopyOnWriteArrayList<>();
    /** 快照操作逻辑 */
    private SnapshotFileOpr<T> snapshotFileOpr;

    public DefaultStateMachine(List<NodeStateChangeListener> listeners) {
        this.listeners.addAll(listeners);
    }

    public DefaultStateMachine() {

    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void init(RaftServerOptions opts) {
        snapshotFileOpr = opts.getSnapshotFileOpr();
    }

    @Override
    public void onApply(Iterator iterator) {
        // do nothing
        while (iterator.hasNext()) {
            info("apply with term: {} and index: {}. ", iterator.getTerm(), iterator.getIndex());
            iterator.next();
        }
    }

    @Override
    public void onLeaderStart(long term) {
        super.onLeaderStart(term);
        leaderTerm.set(term);
        for (NodeStateChangeListener listener : listeners) {
            try {
                listener.onBecomeLeader(term);
            } catch (Exception e) {
                error("listener encounter error, ", e);
            }
        }
    }

    @Override
    public void onLeaderStop(Status status) {
        super.onLeaderStop(status);
        long oldTerm = leaderTerm.get();
        leaderTerm.set(-1L);
        for (NodeStateChangeListener listener : listeners) {
            listener.onStepDown(oldTerm);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        Object snapshotValue = getSnapshotValue();
        Utils.runInThread(() -> {
            String path = writer.getPath() + File.separator + RaftUtils.SNAPSHOT_FILE_NAME;
            if (snapshotFileOpr.save(path, getSnapshotValue())) {
                if (writer.addFile(RaftUtils.SNAPSHOT_FILE_NAME)) {
                    done.run(Status.OK());
                } else {
                    done.run(new Status(RaftError.EIO, "fail to add file to writer"));
                }
            } else {
                done.run(new Status(RaftError.EIO, "fail to save snapshot %s", path));
            }
        });
    }

    @Override
    public void onError(RaftException e) {
        error("raft error: {}", e, e);
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        if (isLeader()) {
            warn("leader is not supposed to load snapshot");
            return false;
        }
        if (reader.getFileMeta(RaftUtils.SNAPSHOT_FILE_NAME) == null) {
            error("fail to find data file in {}", reader.getPath());
            return false;
        }

        String path = reader.getPath() + File.separator + RaftUtils.SNAPSHOT_FILE_NAME;
        try {
            onSnapshotLoad(snapshotFileOpr.load(path));
            return true;
        } catch (IOException e) {
            error("fail to load snapshot from {}", path);
            return false;
        }
    }

    /**
     * 获取快照保存的值
     */
    protected T getSnapshotValue() {
        return null;
    }

    /**
     * 快照加载完后, 交给实现类完成赋值
     */
    protected void onSnapshotLoad(T obj) {

    }

    /**
     * 当前节点是否是leader
     */
    public boolean isLeader() {
        return leaderTerm.get() > 0;
    }

    /**
     * 注册{@link NodeStateChangeListener}实例
     */
    public void addListeners(NodeStateChangeListener... listeners) {
        addListeners(Arrays.asList(listeners));
    }

    /**
     * 注册{@link NodeStateChangeListener}实例
     */
    public void addListeners(Collection<NodeStateChangeListener> listeners) {
        this.listeners.addAll(listeners);
    }
}
