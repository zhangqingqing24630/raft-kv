package cn.think.in.java.netty.server;

import cn.think.in.java.current.RaftThreadPoolExecutor;
import cn.think.in.java.impl.DefaultNode;
import cn.think.in.java.netty.codec.JSONSerializer;
import cn.think.in.java.netty.codec.RpcDecoder;
import cn.think.in.java.netty.codec.RpcEncoder;
import cn.think.in.java.netty.handler.ServerHandler;
import cn.think.in.java.rpc.Request;
import cn.think.in.java.rpc.Response;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServer {
    EventLoopGroup boss;
    EventLoopGroup worker;
    private ServerBootstrap bootstrap;
    private String serverAddress;
    private Integer serverPort;
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftThreadPoolExecutor.class);
    private NettyServer() {
    }

    public static NettyServer getInstance() {
        return RaftRpcServerHolder.INSTANCE;
    }

    private static class RaftRpcServerHolder {
        private static NettyServer INSTANCE = new NettyServer();
    }

    /**
     * 服务提供者采用的是主从 Reactor 线程模型，启动过程包括配置线程池、Channel 初始化、端口绑定三个步骤。
     */
    public void startRpcServer(DefaultNode node) throws Exception {
        boss = new NioEventLoopGroup();
        worker = new NioEventLoopGroup();
        serverAddress = "localhost";
        serverPort = node.getConfig().getSelfPort();
        try {
            bootstrap = new ServerBootstrap();
            bootstrap.group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new RpcDecoder(Request.class, new JSONSerializer()))
                                    .addLast(new RpcEncoder(Response.class, new JSONSerializer()))
                                    .addLast(new ServerHandler(node));
                        }
                    }).childOption(ChannelOption.SO_KEEPALIVE, true);
            ChannelFuture channelFuture = bootstrap.bind(serverAddress, serverPort).sync();
            LOGGER.info("server addr {} started on port {}", serverAddress, serverPort);
            node.add();


            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                synchronized (node) {
                    node.notifyAll();

                    try {
                        node.destroy();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }

                }
            }));
            channelFuture.channel().closeFuture().sync();
        } finally {
            destroy();
        }
    }

    public void destroy() {
        boss.shutdownGracefully();
        worker.shutdownGracefully();
    }
}
