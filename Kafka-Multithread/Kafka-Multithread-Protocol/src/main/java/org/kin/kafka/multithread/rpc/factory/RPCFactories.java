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

    public static void service(Class protocol, Object service, String registryAddress, String protocolName){
        if(factory == null){
            init(DEFAULT_RPCFACTORY);
        }
        factory.service(protocol, service, registryAddress, protocolName);
    }

    public static void serviceWithoutRegistry(Class service, Object serviceImpl, String protocolName, int port){
        if(factory == null){
            init(DEFAULT_RPCFACTORY);
        }
        factory.serviceWithoutRegistry(service, serviceImpl, protocolName, port);
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
