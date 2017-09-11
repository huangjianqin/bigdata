package org.kin.kafka.multithread.rpc.factory.impl;

import com.alibaba.dubbo.config.*;
import org.kin.kafka.multithread.rpc.factory.RPCFactory;

/**
 * Created by huangjianqin on 2017/9/8.
 * 默认RPC工厂
 * dubbo直连
 */
public class DefaultRPCFactoryImpl implements RPCFactory {
    @Override
    public String type() {
        return "DEFAULT";
    }

    @Override
    public void service(Class service, Object serviceImpl, String registryAddress, String protocolName){
        ApplicationConfig applicationConfig = new ApplicationConfig();
        applicationConfig.setName(service.getName() + "-service" + applicationConfig.getId());

        RegistryConfig registryConfig = new RegistryConfig();
        registryConfig.setAddress(registryAddress);

        ProtocolConfig protocolConfig = new ProtocolConfig();
        protocolConfig.setName(protocolName);

        ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setInterface(service.getName());
        serviceConfig.setRef(serviceImpl);
        serviceConfig.setRegistry(registryConfig);
        serviceConfig.setProtocol(protocolConfig);
        serviceConfig.setApplication(applicationConfig);

        serviceConfig.export();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
               serviceConfig.unexport();
            }
        }));
    }

    @Override
    public void serviceWithoutRegistry(Class service, Object serviceImpl, String protocolName, int port){
        ApplicationConfig applicationConfig = new ApplicationConfig();
        applicationConfig.setName(service.getName() + "-service" + applicationConfig.getId());

        ProtocolConfig protocolConfig = new ProtocolConfig();
        protocolConfig.setName(protocolName);
        protocolConfig.setPort(port);

        ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setInterface(service.getName());
        serviceConfig.setRef(serviceImpl);
        serviceConfig.setProtocol(protocolConfig);
        serviceConfig.setApplication(applicationConfig);

        serviceConfig.export();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                serviceConfig.unexport();
            }
        }));
    }

    @Override
    public <T> T client(Class service, String registryAddress){
        ApplicationConfig applicationConfig = new ApplicationConfig();
        applicationConfig.setName(service.getName() + "-client" + applicationConfig.getId());

        RegistryConfig registryConfig = new RegistryConfig();
        registryConfig.setAddress(registryAddress);

        ReferenceConfig referenceConfig = new ReferenceConfig();
        referenceConfig.setApplication(applicationConfig);
        referenceConfig.setRegistry(registryConfig);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                referenceConfig.destroy();
            }
        }));

        return (T) referenceConfig.get();
    }

    @Override
    public <T> T clientWithoutRegistry(Class service, String host, int port){
        ReferenceConfig referenceConfig = new ReferenceConfig();
        referenceConfig.setUrl("dubbo://" + host + ":" + port + "/" + service.getName());

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                referenceConfig.destroy();
            }
        }));

        return (T) referenceConfig.get();
    }
}
