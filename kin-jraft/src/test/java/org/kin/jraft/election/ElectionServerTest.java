package org.kin.jraft.election;

import org.kin.jraft.NodeStateChangeListener;
import org.kin.jraft.RaftServerBootstrap;
import org.kin.jraft.RaftServerOptions;

import java.util.concurrent.TimeUnit;

/**
 * @author huangjianqin
 * @date 2021/11/8
 */
public class ElectionServerTest {
    public static void main(String[] args) throws InterruptedException {
        String address = args[0];
        String clusterAddresses = args[1];

        String[] strs = address.split(":");

        RaftServerBootstrap bootstrap = RaftServerOptions.electionBuilder()
                .groupId("election_raft")
                //模拟每个节点的log目录不一致
                .dataDir("raft/election".concat(strs[1]))
                .address(address)
                .clusterAddresses(clusterAddresses)
                .listeners(new NodeStateChangeListener() {

                    @Override
                    public void onBecomeLeader(long term) {
                        System.out.println("[ElectionBootstrap] Leader start on term: " + term);
                    }

                    @Override
                    public void onStepDown(long oldTerm) {
                        System.out.println("[ElectionBootstrap] Leader step down: " + oldTerm);
                    }
                })
                .bootstrap();

        Thread.sleep(TimeUnit.MINUTES.toMillis(5));
    }
}
