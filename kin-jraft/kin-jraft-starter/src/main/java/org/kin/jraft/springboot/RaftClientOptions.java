package org.kin.jraft.springboot;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author huangjianqin
 * @date 2021/11/24
 */
@ConfigurationProperties("kin.jraft.client")
public class RaftClientOptions extends org.kin.jraft.RaftClientOptions {
}
