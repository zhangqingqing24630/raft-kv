package cn.think.in.java.netty.handler;

import cn.think.in.java.impl.DefaultNode;
import cn.think.in.java.netty.client.RpcFuture;
import cn.think.in.java.rpc.Response;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class ClientHandler extends SimpleChannelInboundHandler<Response> {

    private final DefaultNode node;

    public ClientHandler(DefaultNode node) {
        this.node = node;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Response msg) throws Exception {
        //客户端只要收到回复，就把任务从未完成队列中移除
        long requestId = msg.getRequestId();
        RpcFuture future = RpcRequestHolder.REQUEST_MAP.remove(requestId);
        future.getPromise().setSuccess(msg);
    }

}
