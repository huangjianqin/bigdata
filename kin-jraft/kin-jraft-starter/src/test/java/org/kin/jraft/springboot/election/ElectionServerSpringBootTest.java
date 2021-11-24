package org.kin.jraft.springboot.election;

import org.kin.jraft.springboot.EnableJRaftServer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.TimeUnit;

/**
 * @author huangjianqin
 * @date 2021/11/8
 */
@EnableJRaftServer
@SpringBootApplication
public class ElectionServerSpringBootTest {
    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(ElectionServerSpringBootTest.class, args);

        Thread.sleep(TimeUnit.MINUTES.toMillis(5));

        context.stop();
    }
}
