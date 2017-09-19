package org.kin.kafka.multithread.configcenter;

import com.alibaba.dubbo.rpc.Protocol;
import org.kin.kafka.multithread.configcenter.config.Config;
import org.kin.kafka.multithread.protocol.configcenter.AdminProtocol;
import org.kin.kafka.multithread.protocol.configcenter.DiamondMasterProtocol;
import org.kin.kafka.multithread.rpc.factory.RPCFactories;

import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/11.
 */
public class DiamondRunner {
    public static void main(String[] args) {
        Diamond diamond = new Diamond();
        Properties config = diamond.getConfig();
        RPCFactories.serviceWithoutRegistry(
                DiamondMasterProtocol.class,
                diamond,
                Integer.valueOf(config.get(Config.DIAMONDMASTERPROTOCOL_PORT).toString())
        );
        RPCFactories.serviceWithoutRegistry(AdminProtocol.class,
                diamond,
                Integer.valueOf(config.get(Config.ADMINPROTOCOL_PORT).toString())
        );

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                diamond.close();
            }
        }));
    }
}
