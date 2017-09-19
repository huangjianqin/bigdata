package org.kin.kafka.multithread.rpc.factory;

/**
 * Created by huangjianqin on 2017/9/8.
 */
public interface RPCFactory {
    String type();
    void service(Class protocol, Object service, String registryAddress, String protocolName, int protocolPort);
    void serviceWithoutRegistry(Class service, Object serviceImpl, int protocolPort);
    void restService(Class service, Object serviceImpl, int protocolPort);
    <T> T client(Class service, String registryAddress);
    <T> T clientWithoutRegistry(Class service, String host, int port);
}
