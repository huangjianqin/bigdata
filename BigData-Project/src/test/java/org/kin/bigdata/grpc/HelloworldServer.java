package org.kin.bigdata.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.kin.bigdata.grpc.demo.helloworld.GreeterGrpc;
import org.kin.bigdata.grpc.demo.helloworld.HelloReply;
import org.kin.bigdata.grpc.demo.helloworld.HelloRequest;

import java.io.IOException;

/**
 * Created by huangjianqin on 2017/12/9.
 */
public class HelloworldServer {
    private Server server;

    public void start() throws IOException {
        int port = 50001;
        this.server = ServerBuilder.forPort(port)
                .addService(new GreeterImpl())
                .build()
                .start();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                server.shutdown();
            }
        });
    }

    public void stop(){
        if(server != null){
            server.shutdown();
        }
    }

    public void awaitShutdown() throws InterruptedException {
        if(server != null){
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        HelloworldServer server = new HelloworldServer();
        server.start();
        server.awaitShutdown();
    }

    static class GreeterImpl extends GreeterGrpc.GreeterImplBase{
        @Override
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            HelloReply reply = HelloReply.newBuilder().setMessage("Hi Hi.").build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
}
