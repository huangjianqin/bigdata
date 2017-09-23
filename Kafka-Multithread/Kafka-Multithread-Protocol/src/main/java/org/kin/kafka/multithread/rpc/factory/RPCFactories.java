package org.kin.kafka.multithread.rpc.factory;

import org.kin.kafka.multithread.rpc.factory.impl.DefaultRPCFactoryImpl;
import org.kin.kafka.multithread.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huangjianqin on 2017/9/8.
 */
public class RPCFactories {
    private static final Logger log = LoggerFactory.getLogger(RPCFactories.class);
    private static RPCFactory factory;
    private static final String DEFAULT_RPCFACTORY = DefaultRPCFactoryImpl.class.getName();

    public static void init(String factoryClass){
        if(factoryClass == null || factoryClass.equals("")){
            factoryClass = DEFAULT_RPCFACTORY;
        }
        log.info("init RPCFactory(class = " + factoryClass + ")");
        factory = (RPCFactory) ClassUtils.instance(factoryClass);
    }

    public static void service(Class protocol, Object service, String registryAddress, String protocolName, int protocolPort){
        if(factory == null){
            init(DEFAULT_RPCFACTORY);
        }
        factory.service(protocol, service, registryAddress, protocolName, protocolPort);
    }

    public static void serviceWithoutRegistry(Class service, Object serviceImpl, int protocolPort){
        if(factory == null){
            init(DEFAULT_RPCFACTORY);
        }
        factory.serviceWithoutRegistry(service, serviceImpl, protocolPort);
    }

    public static void restServiceWithoutRegistry(Class service, Object serviceImpl, int protocolPort) {
        if(factory == null){
            init(DEFAULT_RPCFACTORY);
        }
        factory.restService(service, serviceImpl, protocolPort);
    }

    public static <T> T client(Class service, String registryAddress){
        if(factory == null){
            init(DEFAULT_RPCFACTORY);
        }
        return factory.client(service, registryAddress);
    }

    public static<T> T clientWithoutRegistry(Class service, String host, int port){
        if(factory == null){
            init(DEFAULT_RPCFACTORY);
        }
        return factory.clientWithoutRegistry(service, host, port);
    }
}
