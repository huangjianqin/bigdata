package org.kin.bigdata.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

/**
 * Created by huangjianqin on 2018/8/17.
 */
public class NginxService {
    public void initService(final int port) {
        EventLoopGroup boss;
        EventLoopGroup worker;
        try {
            boss = new NioEventLoopGroup(1);
            worker = new NioEventLoopGroup(2);

            EventLoopGroup finalBoss = boss;
            EventLoopGroup finalWorker = worker;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                finalBoss.shutdownGracefully();
                finalWorker.shutdownGracefully();
            }));

            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer() {
                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            channel.pipeline().addLast(new SimpleChannelInboundHandler() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
                                    String rec = new String(ByteBufUtil.getBytes((ByteBuf) o));
                                    System.out.println("服务(port=" + port + ")接收消息(" + rec.toString() + ")成功");
                                    if (rec.equals("end")) {
                                        channelHandlerContext.channel().close().sync();
                                    }
                                }
                            });
                        }
                    });

            ChannelFuture channelFuture = serverBootstrap.bind(new InetSocketAddress("10.9.0.64", port));
            channelFuture.addListener((ChannelFuture cf) -> {
                if (cf.isSuccess()) {
                    System.out.println("服务(port=" + port + ")开启");
                }
            });
            channelFuture.sync();

            ChannelFuture closeFuture = channelFuture.channel().closeFuture();
            closeFuture.addListener((ChannelFuture cf) -> {
                if (cf.isSuccess()) {
                    System.out.println("服务(port=" + port + ")结束");
                    finalBoss.shutdownGracefully();
                    finalWorker.shutdownGracefully();
                }
            });
            closeFuture.sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void sendNginxServerMessage(int port, int num) {
        EventLoopGroup group = new NioEventLoopGroup(2);
        Bootstrap bootstrap = new Bootstrap();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            group.shutdownGracefully();
        }));

        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer() {
                    @Override
                    protected void initChannel(Channel channel) throws Exception {
                        channel.pipeline().addLast(new SimpleChannelInboundHandler() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
                                System.out.println(new String(ByteBufUtil.getBytes((ByteBuf) o)));
                            }
                        });
                        channel.pipeline().addLast(new ChannelOutboundHandlerAdapter() {
                            @Override
                            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                                ctx.writeAndFlush(Unpooled.copiedBuffer(msg.toString().getBytes())).addListener(new ChannelFutureListener() {
                                    @Override
                                    public void operationComplete(ChannelFuture channelFuture) throws Exception {
                                        if (channelFuture.isSuccess()) {
                                            System.out.println("发送消息(" + msg.toString() + ")成功");
                                        }
                                    }
                                });
                            }
                        });
                    }
                });
        ChannelFuture channelFuture = bootstrap.connect(new InetSocketAddress(port));
        channelFuture.addListener((ChannelFuture cf) -> {
            if (cf.isSuccess()) {
                System.out.println("连接成功");
                for (int i = 0; i < num; i++) {
                    cf.channel().writeAndFlush("message" + i);
                }
                cf.channel().writeAndFlush("end");
            }
        });

        try {
            ChannelFuture closeFuture = channelFuture.channel().closeFuture();
            channelFuture.addListener((ChannelFuture cf) -> {
                if (cf.isSuccess()) {
                    System.out.println("客户端结束");
                    group.shutdownGracefully();
                }
            });
            closeFuture.sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        NginxService nginxService = new NginxService();
//        new Thread(() -> nginxService.initService(8501)).start();
//        new Thread(() -> nginxService.initService(8502)).start();

//        Thread.sleep(5000);

        new Thread(() -> nginxService.sendNginxServerMessage(8500, 1)).start();
    }
}
