package org.kin.hbase.starter;

import org.kin.framework.utils.StringUtils;
import org.kin.hbase.core.HBasePool;
import org.kin.hbase.core.config.HBaseConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by huangjianqin on 2018/5/26.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(HBaseZkProperties.class)
@EnableConfigurationProperties(HBaseZkProperties.class)
public class HBaseAutoConfiguration {

    @Autowired
    private HBaseZkProperties hbaseZkProperties;

    @Bean
    public HBasePool hBasePool() throws Exception {
        HBasePool defaultPool = HBasePool.common();
        //single HBase
        if (StringUtils.isNotBlank(hbaseZkProperties.getQuorum())) {
            HBaseConfig config = new HBaseConfig();
            config.setZookeeperQuorum(config.getZookeeperQuorum());

            defaultPool.initializeConnections(config);
        }

        return defaultPool;
    }
}
