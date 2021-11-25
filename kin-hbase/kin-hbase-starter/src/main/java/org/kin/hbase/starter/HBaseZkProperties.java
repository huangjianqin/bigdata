package org.kin.hbase.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Created by huangjianqin on 2018/5/26.
 */
@ConfigurationProperties(prefix = "kin.hbase.zookeeper")
public class HBaseZkProperties {
    /**
     * zookeeper地址
     */
    private String quorum;

    //setter & getter
    public String getQuorum() {
        return quorum;
    }

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }
}
