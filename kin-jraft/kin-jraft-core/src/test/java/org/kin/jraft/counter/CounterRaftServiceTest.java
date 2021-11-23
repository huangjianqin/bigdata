package org.kin.jraft.counter;

import org.kin.jraft.NodeStateChangeListener;
import org.kin.jraft.RaftServerBootstrap;
import org.kin.jraft.RaftServerOptions;
import org.kin.jraft.counter.processor.GetValueRequestProcessor;
import org.kin.jraft.counter.processor.IncrementAndGetRequestProcessor;

import java.util.concurrent.TimeUnit;

/**
 * @author huangjianqin
 * @date 2021/11/8
 */
public class CounterRaftServiceTest {
    public static void main(String[] args) throws InterruptedException {
        String address = args[0];
        String clusterAddresses = args[1];

        String[] strs = address.split(":");

        RaftServerBootstrap bootstrap = RaftServerOptions.builder()
                .groupId("counter_raft")
                //模拟每个节点的log目录不一致
                .dataDir("raft/counter".concat(strs[1]))
                .address(address)
                .clusterAddresses(clusterAddresses)
                .electionTimeoutMs(1000)
                .disableCli()
                .snapshotIntervalSecs(30)
                .raftServiceFactory((b, s) -> {
                    CounterRaftService counterService = new CounterRaftServiceImpl(b);
                    s.registerProcessor(new GetValueRequestProcessor(counterService));
                    s.registerProcessor(new IncrementAndGetRequestProcessor(counterService));
                    return counterService;
                })
                .stateMachineFactory((b, s) -> new CounterStateMachine())
                .listeners(new NodeStateChangeListener() {

                    @Override
                    public void onBecomeLeader(long term) {
                        System.out.println("[CounterBootstrap] Leader start on term: " + term);
                    }

                    @Override
                    public void onStepDown(long oldTerm) {
                        System.out.println("[CounterBootstrap] Leader step down: " + oldTerm);
                    }
                })
                .bootstrap();

        Thread.sleep(TimeUnit.MINUTES.toMillis(5));
    }
}
