package cn.think.in.java.netty.handler;

import cn.think.in.java.common.Peer;
import cn.think.in.java.common.PeerSet;
import cn.think.in.java.current.RaftThreadPoolExecutor;
import cn.think.in.java.entity.AentryParam;
import cn.think.in.java.entity.AentryResult;
import cn.think.in.java.entity.RvoteParam;
import cn.think.in.java.entity.RvoteResult;
import cn.think.in.java.impl.DefaultNode;
import cn.think.in.java.membership.changes.Result;
import cn.think.in.java.rpc.Request;
import cn.think.in.java.rpc.Response;
import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.client.ClientKVAck;
import raft.client.ClientKVReq;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class ServerHandler extends SimpleChannelInboundHandler<Request> {

    private DefaultNode node;
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerHandler.class);
    public ServerHandler(DefaultNode node) {
        this.node = node;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Request msg)  {
        //LOGGER .info(JSON.toJSONString(msg));
        Response response = new Response();
        response.setRequestId(msg.getRequestId());
        switch (msg.getCmd()) {
            case Request.R_VOTE:
                RvoteResult rvoteResult = node.handlerRequestVote(JSON.parseObject(JSON.toJSONString(msg.getObj()), RvoteParam.class));
                response.setResult(rvoteResult);
                break;
            case Request.A_ENTRIES:
                AentryResult aentryResult = node.handlerAppendEntries(JSON.parseObject(JSON.toJSONString(msg.getObj()), AentryParam.class));
                response.setResult(aentryResult);
                break;
            case Request.CLIENT_REQ:
                //客户端请求
                ClientKVAck clientKVAck = null;
                try {
                    clientKVAck = node.handlerClientRequest(JSON.parseObject(JSON.toJSONString(msg.getObj()), ClientKVReq.class));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                response.setResult(clientKVAck);
                break;
            case Request.CHANGE_CONFIG_ADD:
                Result addPeerResult = null;
                try {
                    addPeerResult = node.addPeer(JSON.parseObject(JSON.toJSONString(msg.getObj()), Peer.class));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                response.setResult(addPeerResult);
                break;
            case Request.CHANGE_PEERSET_ADD:
                Result addPeerSetResult = node.addPeerSet(JSON.parseObject(JSON.toJSONString(msg.getObj()), PeerSet.class));
                response.setResult(addPeerSetResult);
                break;

            case Request.CHANGE_CONFIG_REMOVE:
                Result removePeerResult = node.removePeer(JSON.parseObject(JSON.toJSONString(msg.getObj()), Peer.class));
                response.setResult(removePeerResult);
                break;
            default:
                LOGGER.error("未知的请求协议，拒绝处理");
                throw new RuntimeException("未知的请求协议");
        }
        ctx.writeAndFlush(response);
    }
}
