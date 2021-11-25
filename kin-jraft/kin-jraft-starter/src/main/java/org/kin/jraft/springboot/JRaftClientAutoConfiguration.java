package org.kin.jraft.springboot;

import org.kin.jraft.RaftClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

/**
 * @author huangjianqin
 * @date 2021/11/24
 */
@Configuration
@ConditionalOnBean(JRaftClientMarkerConfiguration.Marker.class)
@EnableConfigurationProperties(RaftClientProperties.class)
public class JRaftClientAutoConfiguration {
    @Resource
    private RaftClientProperties clientOptions;

    @Bean
    public RaftClient raftClient() {
        RaftClient raftClient = new RaftClient();
        raftClient.init(clientOptions);
        return raftClient;
    }
}
