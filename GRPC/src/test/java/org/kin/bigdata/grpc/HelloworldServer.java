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
    //server
    private Server server;

    public void start() throws IOException {
        int port = 50001;
        //创建server
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
            //阻塞等待关闭
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        HelloworldServer server = new HelloworldServer();
        server.start();
        server.awaitShutdown();
    }

    //实现由IDL自动生成的服务接口
    static class GreeterImpl extends GreeterGrpc.GreeterImplBase{
        @Override
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            HelloReply reply = HelloReply.newBuilder().setMessage("Hi Hi.").build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void sayMore(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            HelloReply reply = HelloReply.newBuilder().setMessage("Hi Hi.").build();
            //堆responses
            responseObserver.onNext(reply);
            responseObserver.onNext(reply);
            responseObserver.onNext(reply);
            //返回responses stream
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<HelloRequest> sayLittle(StreamObserver<HelloReply> responseObserver) {
            return new StreamObserver<HelloRequest>() {
                private String all = "";

                @Override
                public void onNext(HelloRequest helloRequest) {
                    all += helloRequest.getMessage();
                }

                @Override
                public void onError(Throwable throwable) {
                    throwable.printStackTrace();
                }

                @Override
                public void onCompleted() {
                    HelloReply reply = HelloReply.newBuilder().setMessage(all).build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            };
        }

        /**
         *
         * @param responseObserver 返回responses的观察者
         * @return
         */
        @Override
        public StreamObserver<HelloRequest> repeat(StreamObserver<HelloReply> responseObserver) {
            //返回一个处理requests的stream观察者
            return new StreamObserver<HelloRequest>() {
                @Override
                public void onNext(HelloRequest helloRequest) {
                    HelloReply reply = HelloReply.newBuilder().setMessage(helloRequest.getMessage()).build();
                    responseObserver.onNext(reply);
                }

                @Override
                public void onError(Throwable throwable) {
                    throwable.printStackTrace();
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
