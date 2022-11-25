/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package raft.client;

import cn.think.in.java.entity.AentryParam;
import cn.think.in.java.impl.DefaultNode;
import cn.think.in.java.netty.client.NettyClient;
import cn.think.in.java.rpc.Request;
import cn.think.in.java.rpc.Response;
import cn.think.in.java.rpc.RpcClient;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class RaftClientRPC {

    //private static List<String> list = Lists.newArrayList("localhost:8776","localhost:8777","localhost:8778","localhost:8779","localhost:8780");
    private static List<String> list = Lists.newArrayList("localhost:8776");
    //private final static RpcClient CLIENT = new DefaultRpcClient();
    private final static NettyClient CLIENT = NettyClient.getInstance();
    private AtomicLong count = new AtomicLong(3);

    public RaftClientRPC() throws Throwable {
        //CLIENT.init();
        CLIENT.startRpcClient(DefaultNode.getInstance());
    }

    /**
     * @param key
     * @return
     */
    public ClientKVAck get(String key) {
        ClientKVReq obj = ClientKVReq.builder().key(key).type(ClientKVReq.GET).build();

        int index = (int) (count.incrementAndGet() % list.size());

        String addr = list.get(index);
        Response response1;
        ClientKVAck response=null;
        Request r = Request.builder().obj(obj).url(addr).cmd(Request.CLIENT_REQ).build();
        try {
                response1 = CLIENT.send(r);
            response=JSON.parseObject(JSON.toJSONString(response1.getResult()), ClientKVAck.class);

        } catch (Exception e) {
        }

        return response;
    }

    /**
     * @param key
     * @param value
     * @return
     */
    public ClientKVAck put(String key, String value) {
        int index = (int) (count.incrementAndGet() % list.size());

        String addr = list.get(index);
        ClientKVReq obj = ClientKVReq.builder().key(key).value(value).type(ClientKVReq.PUT).build();

        Request r = Request.builder().obj(obj).url(addr).cmd(Request.CLIENT_REQ).build();
        Response response1=null;
        ClientKVAck response=null;
        try {
                response1 = CLIENT.send(r);
                response=JSON.parseObject(JSON.toJSONString(response1.getResult()), ClientKVAck.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return response;
    }
}
