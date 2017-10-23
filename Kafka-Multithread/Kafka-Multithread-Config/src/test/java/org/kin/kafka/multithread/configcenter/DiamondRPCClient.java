package org.kin.kafka.multithread.configcenter;

import org.junit.Test;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.configcenter.utils.YAMLUtils;
import org.kin.kafka.multithread.domain.ConfigFetcherHeartbeatResponse;
import org.kin.kafka.multithread.domain.ConfigFetcherHeartbeatRequest;
import org.kin.kafka.multithread.protocol.app.ApplicationContextInfo;
import org.kin.kafka.multithread.protocol.configcenter.DiamondMasterProtocol;
import org.kin.kafka.multithread.rpc.factory.RPCFactories;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/10/17.
 */
public class DiamondRPCClient extends TestConfigBase{

    public DiamondMasterProtocol client(){
        return RPCFactories.clientWithoutRegistry(DiamondMasterProtocol.class, "localhost", 60001);
    }

    @Test
    public void heartbeat(){
        ConfigFetcherHeartbeatRequest heartbeat = new ConfigFetcherHeartbeatRequest(
                new ApplicationContextInfo("test1", "localhost"),
                Collections.singletonList("test1"),
                new ArrayList<>(),
                System.currentTimeMillis()
        );
        DiamondMasterProtocol client = client();
        client.heartbeat(heartbeat);
    }
}
