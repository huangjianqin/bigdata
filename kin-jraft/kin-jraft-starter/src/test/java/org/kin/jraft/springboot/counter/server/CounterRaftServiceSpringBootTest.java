package org.kin.jraft.springboot.counter.server;

import org.kin.jraft.NodeStateChangeListener;
import org.kin.jraft.RaftServiceFactory;
import org.kin.jraft.StateMachineFactory;
import org.kin.jraft.springboot.EnableJRaftServer;
import org.kin.jraft.springboot.counter.processor.GetValueRequestProcessor;
import org.kin.jraft.springboot.counter.processor.IncrementAndGetRequestProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.TimeUnit;

/**
 * @author huangjianqin
 * @date 2021/11/8
 */
@EnableJRaftServer
@SpringBootApplication
public class CounterRaftServiceSpringBootTest {
    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(CounterRaftServiceSpringBootTest.class, args);

        Thread.sleep(TimeUnit.MINUTES.toMillis(5));
        context.stop();
    }

    @Bean
    public NodeStateChangeListener printListener() {
        return new NodeStateChangeListener() {

            @Override
            public void onBecomeLeader(long term) {
                System.out.println("[CounterBootstrap] Leader start on term: " + term);
            }

            @Override
            public void onStepDown(long oldTerm) {
                System.out.println("[CounterBootstrap] Leader step down: " + oldTerm);
            }
        };
    }

    @Bean
    public RaftServiceFactory<CounterRaftService> counterRaftServiceFactory() {
        return (b, s) -> {
            CounterRaftService counterService = new CounterRaftServiceImpl(b);
            s.registerProcessor(new GetValueRequestProcessor(counterService));
            s.registerProcessor(new IncrementAndGetRequestProcessor(counterService));
            return counterService;
        };
    }

    @Bean
    public StateMachineFactory<CounterStateMachine, CounterRaftService> counterStateMachineFactory() {
        return (b, s) -> new CounterStateMachine();
    }
}
