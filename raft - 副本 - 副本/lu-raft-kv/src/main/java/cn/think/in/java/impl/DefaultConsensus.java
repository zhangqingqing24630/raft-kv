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

import cn.think.in.java.Consensus;
import cn.think.in.java.common.NodeStatus;
import cn.think.in.java.common.Peer;
import cn.think.in.java.entity.*;
import io.netty.util.internal.StringUtil;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * 默认的一致性模块实现.
 */
@Setter
@Getter
public class DefaultConsensus implements Consensus {


    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConsensus.class);


    public final DefaultNode node;

    public final ReentrantLock voteLock = new ReentrantLock();
    public final ReentrantLock appendLock = new ReentrantLock();

    public DefaultConsensus(DefaultNode node) {
        this.node = node;
    }

    /**
     * 请求投票 RPC
     *
     * 接收者实现：
     *      如果term < currentTerm返回 false
     *      如果 votedFor 为空（同轮一票不能多投）或者就是 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他
     */
    @Override
    public RvoteResult requestVote(RvoteParam param) {
        try {
            RvoteResult.Builder builder = RvoteResult.newBuilder();
            //选举操作应该是串行的，因为涉及到状态修改，并发操作将导致数据错乱。也就是说，如果抢锁失败，应当立即返回错误。
            if (!voteLock.tryLock()) {
                return builder.term(node.getCurrentTerm()).voteGranted(false).build();
            }

            // 竞选者任期号没有自己新
            if (param.getTerm() < node.getCurrentTerm()) {
                LOGGER.info("竞选者任期号没有自己新，投票失败");
                return builder.term(node.getCurrentTerm()).voteGranted(false).build();
            }
            //对于同一个任期，每个服务器节点只会投给一个 candidate ，按照先来先服务
            if(!StringUtil.isNullOrEmpty(node.getVotedFor()))
                LOGGER.info("我已经把票投给了 [{}],不能一票多投", node.getVotedFor());
            LOGGER.info("我的任期是 {}, 候选人的任期是 : {}", node.getCurrentTerm(), param.getTerm());
            // (当前节点并没有投票 或者 已经投票过了且是对方节点) && 对方日志和自己一样新，才会把票投给竞选者
            //如果当前节点没有投票给任何人，或者投的正好是对方，那么就可以比较日志的大小，反之，返回失败
            if ((StringUtil.isNullOrEmpty(node.getVotedFor()) || node.getVotedFor().equals(param.getCandidateId()))) {

                if (node.getLogModule().getLast() != null) {
                    // 竞选者最后一条日志的任期号没有自己新
                    if (node.getLogModule().getLast().getTerm() > param.getLastLogTerm()) {
                        LOGGER.info("竞选者最后一条日志的任期号没有自己新，投票失败");
                        return RvoteResult.fail();
                    }
                    // 竞选者最后一条日志的索引没有自己新
                    if (node.getLogModule().getLastIndex() > param.getLastLogIndex()) {
                        LOGGER.info("竞选者最后一条日志的索引没有自己新，投票失败");
                        return RvoteResult.fail();
                    }
                }
                LOGGER.info("投票给候选者，自己并变成 follower");
                node.status = NodeStatus.FOLLOWER;
                node.peerSet.setLeader(new Peer(param.getCandidateId()));
                node.setCurrentTerm(param.getTerm());
                node.setVotedFor(param.getServerId());
                // 返回成功
                return builder.term(node.currentTerm).voteGranted(true).build();
            }

            return builder.term(node.currentTerm).voteGranted(false).build();

        } finally {
            voteLock.unlock();
        }
    }


    /**
     * 附加日志(多个日志,为了提高效率) RPC
     *
     * 接收者实现：
     *    如果 term < currentTerm 就返回 false
     *    如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false
     *    如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的
     *    附加任何在已有的日志中不存在的条目
     *    如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
     */
    @Override
    public AentryResult appendEntries(AentryParam param) {
        System.out.println(node.peerSet.getList());
        AentryResult result = AentryResult.fail();
        try {
            if (!appendLock.tryLock()) {
                return result;
            }
//无论成功失败首先设置返回值，也就是将自己的 term 返回给 leader
            result.setTerm(node.getCurrentTerm());
            // 如果leader term 小于自身，返回失败。不更新选举时间和心跳时间。触发选举
            if (param.getTerm() < node.getCurrentTerm()) {
                return result;
            }
//在收到心跳或者投票给别人后，都将 preElectionTime 设为当前时间，达到延长的效果
            node.preHeartBeatTime = System.currentTimeMillis();
            node.preElectionTime = System.currentTimeMillis();
            node.peerSet.setLeader(new Peer(param.getLeaderId()));

            // 判断对方的 term 是否大于自身，如果大于自身，变成 follower，这样如果我还在选举，就退出。同时更新选举时间和心跳时间。
            if (param.getTerm() >= node.getCurrentTerm()) {
//                LOGGER.debug("node {} 收到心跳 变成 FOLLOWER, 我的任期 : {}, leader任期 : {}, param serverId",
//                        node.peerSet.getSelf(), node.currentTerm, param.getTerm(), param.getServerId());
                // 认怂
                node.status = NodeStatus.FOLLOWER;
            }
            // 使用对方的 term.
            node.setCurrentTerm(param.getTerm());

            //心跳
            if (param.getEntries() == null || param.getEntries().length == 0) {
                LOGGER.info("node {} 成功收到心跳 , leader term : {}, my term : {}",
                        param.getServerId(), param.getTerm(), node.getCurrentTerm());
                return AentryResult.newBuilder().term(node.getCurrentTerm()).success(true).build();
            }



            // 真实日志
            if (node.getLogModule().getLastIndex() != 0 && param.getPrevLogIndex() != 0) {
                LogEntry logEntry;
                if ((logEntry = node.getLogModule().read(param.getPrevLogIndex())) != null) {
                    // 如果日志在 prevLogIndex（理论已经同步的日志） 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false
                    // 需要减小 nextIndex 重试.
                    if (logEntry.getTerm() != param.getPreLogTerm()) {
                        LOGGER.info("一致性检查，任期不匹配，追加日志失败");
                        return result;
                    }
                } else {
                    LOGGER.info("一致性检查，index不匹配，追加日志失败");
                    // index 不对,根据要存储日志的前一个索引，查找不到， 需要递减 nextIndex 重试.
                    return result;
                }

            }

            // 如果要放入的位置和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的
            LogEntry existLog = node.getLogModule().read(((param.getPrevLogIndex() + 1)));
            if (existLog != null && existLog.getTerm() != param.getEntries()[0].getTerm()) {
                // 删除这一条和之后所有的日志（leader的日志是不动的，其他的flower想办法与我一致）
                LOGGER.info("一致性检查，日志冲突，追加日志失败");
                node.getLogModule().removeOnStartIndex(param.getPrevLogIndex() + 1);
            } else if (existLog != null) {
                // 已经有日志了, 不能重复写入.
                result.setSuccess(true);
                LOGGER.info("追加日志成功");
                return result;
            }

            // 进行一致性检查后，写进日志覆盖原有的值，并且应用到follower状态机
            for (LogEntry entry : param.getEntries()) {
                node.getLogModule().write(entry);
                node.stateMachine.apply(entry);
                result.setSuccess(true);
                LOGGER.info("追加日志成功");
            }

            //如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
            if (param.getLeaderCommit() > node.getCommitIndex()) {
                //领导人已经提交的日志的索引值>本节点已经被提交的日志条目的索引值，leader的日志还没有同步完
                int commitIndex = (int) Math.min(param.getLeaderCommit(), node.getLogModule().getLastIndex());
                node.setCommitIndex(commitIndex);//已知的最大的已经被提交的日志条目的索引值
                node.setLastApplied(commitIndex);//最后被应用到状态机的日志条目索引值
            }

            result.setTerm(node.getCurrentTerm());

            node.status = NodeStatus.FOLLOWER;
            return result;
        } finally {
            appendLock.unlock();
        }
    }


}
