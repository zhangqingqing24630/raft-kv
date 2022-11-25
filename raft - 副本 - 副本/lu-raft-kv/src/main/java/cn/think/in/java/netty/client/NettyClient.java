package cn.think.in.java.netty.client;

import cn.think.in.java.impl.DefaultNode;
import cn.think.in.java.netty.codec.JSONSerializer;
import cn.think.in.java.netty.codec.RpcDecoder;
import cn.think.in.java.netty.codec.RpcEncoder;
import cn.think.in.java.netty.handler.ClientHandler;
import cn.think.in.java.netty.handler.RpcRequestHolder;
import cn.think.in.java.rpc.Request;
import cn.think.in.java.rpc.Response;
import cn.think.in.java.rpc.RpcClient;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NettyClient implements RpcClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);
    private Bootstrap bootstrap;
    private EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

    public NettyClient() {
    }

    public static NettyClient getInstance() {
        return RpcClientHolder.INSTANCE;
    }

    private static class RpcClientHolder {
        private static NettyClient INSTANCE = new NettyClient();
    }

    /**
     * 启动Client
     */
    public void startRpcClient(DefaultNode node) {
        bootstrap = new Bootstrap();
        bootstrap
                .group(eventLoopGroup)
                // 指定Channel
                .channel(NioSocketChannel.class)
                // 将小的数据包包装成更大的帧进行传送，提高网络的负载,即TCP延迟传输
                .option(ChannelOption.SO_KEEPALIVE, true)
                // 将小的数据包包装成更大的帧进行传送，提高网络的负载,即TCP延迟传输
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new RpcDecoder(Response.class, new JSONSerializer()))
                                .addLast(new RpcEncoder(Request.class, new JSONSerializer()))
                                .addLast(new ClientHandler(node));
                    }
                });
    }

    /**
     * 发送消息
     */
    @Override
    public Response send(Request request) throws ExecutionException, InterruptedException, TimeoutException {
        return send(request, (int) TimeUnit.SECONDS.toMillis(3000));
    }
    @Override
    public Response send(Request request, int timeout) throws InterruptedException, ExecutionException, TimeoutException {
        String[] addrs = request.getUrl().split(":");
        String host = addrs[0];
        int port = Integer.valueOf(addrs[1]);
        RpcFuture<Response> rpcFuture = new RpcFuture<>(new DefaultPromise<>(new DefaultEventLoop()), 3000);

        ChannelFuture future = bootstrap.connect(host, port).sync();

        Long requestId = RpcRequestHolder.REQUEST_ID_GEN.incrementAndGet();
        request.setRequestId(requestId);
        RpcRequestHolder.REQUEST_MAP.put(requestId, rpcFuture);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    //LOGGER.info("自定义rpc发送成功");
                } else {
                    //LOGGER.error("自定义rpc发送失败");
                }
            }
        });
        future.channel().writeAndFlush(request);
        return rpcFuture.getPromise().get(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void init() throws Throwable {

    }

    public void destroy() {
        eventLoopGroup.shutdownGracefully();
    }
}
