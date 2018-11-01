package org.kin.hbase.starter;

import org.kin.framework.utils.StringUtils;
import org.kin.hbase.core.HBasePool;
import org.kin.hbase.core.config.HBaseConfig;
import org.kin.hbase.core.domain.Constants;
import org.kin.hbase.starter.config.SpringBootHBaseConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by huangjianqin on 2018/5/26.
 */
@Configuration
@ConditionalOnClass(SpringBootHBaseConfig.class)
@EnableConfigurationProperties(SpringBootHBaseConfig.class)
public class HBaseAutoConfiguration {

    @Autowired
    private SpringBootHBaseConfig springBootHBaseConfig;

    @Bean
    public HBasePool hBasePool() throws Exception {
        HBasePool defaultPool = HBasePool.common();
        //single HBase
        if (StringUtils.isNotBlank(springBootHBaseConfig.getZookeeperQuorum())) {
            HBaseConfig config = new HBaseConfig();
            config.setZookeeperQuorum(config.getZookeeperQuorum());
            config.setZookeeperClientPort(
                    StringUtils.isBlank(config.getZookeeperClientPort()) ? Constants.DEFAULT_HBASE_PORT :
                            config.getZookeeperClientPort());

            defaultPool.initializeConnections(config);
        }

        return defaultPool;
    }
}
