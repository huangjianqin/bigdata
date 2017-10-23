package org.kin.kafka.multithread.rpc.factory.impl;

import com.alibaba.dubbo.config.*;
import org.kin.kafka.multithread.rpc.factory.RPCFactory;
import org.kin.kafka.multithread.utils.HostUtils;

/**
 * Created by huangjianqin on 2017/9/8.
 * 默认RPC工厂
 * dubbo直连
 */
public class DefaultRPCFactoryImpl implements RPCFactory {
    @Override
    public String type() {
        return "dubbo";
    }

    @Override
    public void service(Class service, Object serviceImpl, String registryAddress, String protocolName, int protocolPort){
        ApplicationConfig applicationConfig = new ApplicationConfig();
        applicationConfig.setName(service.getSimpleName() + "-" + HostUtils.localhost());

        RegistryConfig registryConfig = new RegistryConfig();
        registryConfig.setAddress(registryAddress);

        ProtocolConfig protocolConfig = new ProtocolConfig();
        protocolConfig.setName(protocolName);
        protocolConfig.setPort(protocolPort);

        ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setInterface(service.getName());
        serviceConfig.setRef(serviceImpl);
        serviceConfig.setRegistry(registryConfig);
        serviceConfig.setProtocol(protocolConfig);
        serviceConfig.setApplication(applicationConfig);
        serviceConfig.setRetries(5);

        serviceConfig.export();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
               serviceConfig.unexport();
            }
        }));
    }

    private void serviceWithoutRegistry(Class service, Object serviceImpl, String protocolName, int protocolPort){
        service(service, serviceImpl, "N/A", protocolName, protocolPort);
    }

    @Override
    public void serviceWithoutRegistry(Class service, Object serviceImpl, int protocolPort){
        serviceWithoutRegistry(service, serviceImpl, "dubbo", protocolPort);
    }

    @Override
    public void restService(Class service, Object serviceImpl, int protocolPort) {
        serviceWithoutRegistry(service, serviceImpl, "rest", protocolPort);
    }

    @Override
    public <T> T client(Class<T> service, String registryAddress){
        ApplicationConfig applicationConfig = new ApplicationConfig();
        applicationConfig.setName(service.getName() + "-client" + applicationConfig.getId());

        RegistryConfig registryConfig = new RegistryConfig();
        registryConfig.setAddress(registryAddress);

        ReferenceConfig referenceConfig = new ReferenceConfig();
        referenceConfig.setInterface(service);
        referenceConfig.setApplication(applicationConfig);
        referenceConfig.setRegistry(registryConfig);
        referenceConfig.setRetries(5);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                referenceConfig.destroy();
            }
        }));

        return (T) referenceConfig.get();
    }

    @Override
    public <T> T clientWithoutRegistry(Class<T> service, String host, int port){
        ApplicationConfig applicationConfig = new ApplicationConfig();
        applicationConfig.setName(service.getName() + "-client" + applicationConfig.getId());

        ReferenceConfig referenceConfig = new ReferenceConfig();
        referenceConfig.setApplication(applicationConfig);
        referenceConfig.setInterface(service);
        referenceConfig.setUrl("dubbo://" + host + ":" + port + "/" + service.getName());
        referenceConfig.setRetries(5);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                referenceConfig.destroy();
            }
        }));

        return (T) referenceConfig.get();
    }
}
