package org.kin.bigdata.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.kin.bigdata.grpc.demo.helloworld.GreeterGrpc;
import org.kin.bigdata.grpc.demo.helloworld.HelloReply;
import org.kin.bigdata.grpc.demo.helloworld.HelloRequest;

import java.util.concurrent.TimeUnit;

/**
 * Created by huangjianqin on 2017/12/11.
 */
public class HelloworldClient {
    private ManagedChannel channel;
    private GreeterGrpc.GreeterBlockingStub stub;

    public HelloworldClient(String host, int port) {
        this(ManagedChannelBuilder
                .forAddress(host, port)
                //默认使用SSL/TLS
                .usePlaintext(true)
                .build()
        );
    }

    public HelloworldClient(ManagedChannel channel) {
        this.channel = channel;
        stub = GreeterGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void greet(String msg){
        HelloRequest request = HelloRequest.newBuilder().setName(msg).build();
        HelloReply reply = stub.sayHello(request);
        System.out.println(reply.getMessage());
    }

    public static void main(String[] args) throws InterruptedException {
        HelloworldClient client = new HelloworldClient("localhost", 50001);
        client.greet("hjq");
        client.shutdown();
    }
}
