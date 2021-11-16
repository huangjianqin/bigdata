package org.kin.jraft;

/**
 * 监听raft node状态变化listener
 *
 * @author huangjianqin
 * @date 2021/11/7
 */
public interface NodeStateChangeListener {
    /**
     * 当前节点成为leader时触发
     *
     * @param term 新leader的term
     */
    default void onBecomeLeader(long term) {
    }

    /**
     * 当前节点step down时触发
     *
     * @param oldTerm 原leader的term
     */
    default void onStepDown(long oldTerm) {
    }
}
