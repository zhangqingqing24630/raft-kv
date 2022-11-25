package cn.think.in.java.netty.handler;

import cn.think.in.java.netty.client.RpcFuture;
import cn.think.in.java.rpc.Response;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RpcRequestHolder {

    public final static AtomicLong REQUEST_ID_GEN = new AtomicLong(0);

    /**
     * 异步回调Map
     */
    public static final Map<Long, RpcFuture<Response>> REQUEST_MAP = new ConcurrentHashMap<>();
}