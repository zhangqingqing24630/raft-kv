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
package cn.think.in.java.impl;

import cn.think.in.java.common.NodeStatus;
import cn.think.in.java.common.Peer;
import cn.think.in.java.common.PeerSet;
import cn.think.in.java.entity.LogEntry;
import cn.think.in.java.membership.changes.ClusterMembershipChanges;
import cn.think.in.java.membership.changes.Result;
import cn.think.in.java.rpc.DefaultRpcClient;
import cn.think.in.java.rpc.Request;
import cn.think.in.java.rpc.Response;
import cn.think.in.java.rpc.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.client.ClientKVAck;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import static com.caucho.hessian.io.HessianInputFactory.log;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * 集群配置变更接口默认实现.
 */
public class ClusterMembershipChangesImpl implements ClusterMembershipChanges {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMembershipChangesImpl.class);


    private final DefaultNode node;

    public ClusterMembershipChangesImpl(DefaultNode node) {
        this.node = node;
    }
    public RpcClient rpcClient = new DefaultRpcClient();
    /**
     * 必须是同步的,一次只能添加一个节点
     *
     * @param newPeer
     */
    @Override
    public synchronized Result addPeer(Peer newPeer) {
        //leader向新节点同步日志
        System.out.println(node.status);
        System.out.println(node.peerSet.getList());
        System.out.println(newPeer.getAddr());
        if(node.peerSet.getList().contains(newPeer)){
            return new Result();
        }else{
            node.peerSet.getList().add(newPeer);
        }
        if (node.status == NodeStatus.LEADER) {
//            node.peerSet.getList().add(newPeer);
            node.nextIndexs.put(newPeer, 0L);
            node.matchIndexs.put(newPeer, 0L);
            for (long i = 0; i < node.logModule.getLastIndex(); i++) {
                LogEntry entry = node.logModule.read(i);
                if (entry != null) {
                    node.replication(newPeer, entry);
                }
            }
            try {
                Thread.sleep(1000);//给同步日志的时间
            } catch (InterruptedException e) {

            }
            log.info("从leader节点向新节点"+newPeer+"同步日志完成");


            //从leader节点把最完整的peerSet同步到其他节点（包括新节点）
            for (Peer ignore : node.peerSet.getPeersWithOutSelf()) {
                // TODO 同步到其他节点.
                Request request = Request.builder()
                        .cmd(Request.CHANGE_PEERSET_ADD)
                        .url(ignore.getAddr())
                        .obj(node.peerSet)
                        .build();

                Response result1 =  rpcClient.send(request);
                Result result = (Result) result1.getResult();
                if (result != null && result.getStatus() == Result.Status.SUCCESS.getCode()) {
                    log.info("从leader节点向"+ignore+"同步peerSet成功");
                } else {
                    log.info("从leader节点向"+ignore+"同步peerSet失败");
                }
            }
        }else{
            Request request = Request.builder()
                    .cmd(Request.CHANGE_CONFIG_ADD)
                    .url(node.getPeerSet().getLeader().getAddr())
                    .obj(newPeer)
                    .build();

            rpcClient.send(request);
        }
        Result result=new Result();
        result.setStatus(Result.SUCCESS);
        return result;
    }

    public synchronized Result addPeerSet(PeerSet newPeerSet) {
        node.peerSet.setList(newPeerSet.getList());
        Result result=new Result();
        result.setStatus(Result.SUCCESS);
        return result;
    }
    /**
     * 必须是同步的,一次只能删除一个节点
     *
     * @param oldPeer
     */
    @Override
    public synchronized Result removePeer(Peer oldPeer) {
        System.out.println(node);
        System.out.println(node.peerSet);
        System.out.println(node.peerSet.getPeersWithOutSelf());
        if (node.peerSet.getPeersWithOutSelf().contains(oldPeer)){
            node.peerSet.getList().remove(oldPeer);
            node.votedFor="";
            LOGGER.info("成功从"+node.peerSet.getSelf()+"的peerSet删除节点"+oldPeer);

        }
        if (node.status == NodeStatus.LEADER){
            if(node.getNextIndexs().containsKey(oldPeer)){
                node.getNextIndexs().remove(oldPeer);
                LOGGER.info("成功从nextIndexs删除节点"+oldPeer);
            }
            if(node.getMatchIndexs().containsKey(oldPeer)){
                node.getMatchIndexs().remove(oldPeer);
                LOGGER.info("成功从MatchIndexs删除节点"+oldPeer);
            }
        }

        Result result=new Result();
        result.setStatus(Result.SUCCESS);
        return result;
    }
}
