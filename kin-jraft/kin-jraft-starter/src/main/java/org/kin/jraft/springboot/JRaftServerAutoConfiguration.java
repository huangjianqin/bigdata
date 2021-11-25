package org.kin.jraft.springboot;

import org.kin.jraft.*;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2021/11/24
 */
@Configuration
@ConditionalOnBean(JRaftServerMarkerConfiguration.Marker.class)
@EnableConfigurationProperties(RaftServerProperties.class)
public class JRaftServerAutoConfiguration {
    @Resource
    private RaftServerProperties serverOptions;

    @Bean
    public <NW extends DefaultStateMachine<?>, S extends RaftService> RaftServerBootstrap raftServerBootstrap(@Autowired List<NodeStateChangeListener> listeners,
                                                                                                              @Autowired RaftServiceFactory<S> raftServiceFactory,
                                                                                                              @Autowired(required = false) StateMachineFactory<NW, S> stateMachineFactory,
                                                                                                              @Autowired(required = false) SnapshotFileOpr<?> snapshotFileOpr) {
        org.kin.jraft.RaftServerOptions.Builder<NW, S> builder = org.kin.jraft.RaftServerOptions.<NW, S>builder()
                .listeners(listeners)
                .raftServiceFactory(raftServiceFactory)
                .stateMachineFactory(stateMachineFactory);
        if (Objects.nonNull(snapshotFileOpr)) {
            builder.snapshotFileOpr(snapshotFileOpr);
        }
        org.kin.jraft.RaftServerOptions<NW, S> realServerOptions = builder.build();

        BeanUtils.copyProperties(serverOptions, realServerOptions);
        RaftServerBootstrap bootstrap = new RaftServerBootstrap();
        bootstrap.init(realServerOptions);
        return bootstrap;
    }

    @ConditionalOnMissingBean(RaftServiceFactory.class)
    @Bean
    public RaftServiceFactory<DefaultRaftService> stateMachineFactory() {
        return RaftServiceFactory.EMPTY;
    }

}
