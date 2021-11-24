package org.kin.jraft.springboot;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author huangjianqin
 * @date 2021/11/24
 */
@Configuration(proxyBeanMethods = false)
public class JRaftServerMarkerConfiguration {
    @Bean
    public JRaftServerMarkerConfiguration.Marker jraftServerMarker() {
        return new JRaftServerMarkerConfiguration.Marker();
    }

    static class Marker {
        Marker() {
        }
    }
}
