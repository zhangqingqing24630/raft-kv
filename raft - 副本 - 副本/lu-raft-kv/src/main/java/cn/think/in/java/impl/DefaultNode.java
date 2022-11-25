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
import cn.think.in.java.LogModule;
import cn.think.in.java.Node;
import cn.think.in.java.StateMachine;
import cn.think.in.java.common.NodeConfig;
import cn.think.in.java.common.NodeStatus;
import cn.think.in.java.common.Peer;
import cn.think.in.java.common.PeerSet;
import cn.think.in.java.current.RaftThreadPool;
import cn.think.in.java.entity.*;
import cn.think.in.java.exception.RaftRemotingException;
import cn.think.in.java.membership.changes.ClusterMembershipChanges;
import cn.think.in.java.membership.changes.Result;
import cn.think.in.java.netty.client.NettyClient;
import cn.think.in.java.netty.server.NettyServer;
import cn.think.in.java.rpc.*;
import cn.think.in.java.util.LongConvert;
import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import raft.client.ClientKVAck;
import raft.client.ClientKVReq;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.think.in.java.common.NodeStatus.LEADER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * 抽象机器节点, 初始为 follower, 角色随时变化.
 */
@Getter
@Setter
@Slf4j
public class DefaultNode implements Node, ClusterMembershipChanges {

    /** 选举时间间隔基数 */
    public volatile long electionTime = 15 * 1000;
    /** 上一次选举时间 */
    public volatile long preElectionTime = 0;

    /** 上次一心跳时间戳 */
    public volatile long preHeartBeatTime = 0;
    /** 心跳间隔基数 */
    public final long heartBeatTick = 5 * 1000;


    private HeartBeatTask heartBeatTask = new HeartBeatTask();
    private ElectionTask electionTask = new ElectionTask();
    private ReplicationFailQueueConsumer replicationFailQueueConsumer = new ReplicationFailQueueConsumer();

    private LinkedBlockingQueue<ReplicationFailModel> replicationFailQueue = new LinkedBlockingQueue<>(2048);


    /**
     * 节点当前状态
     *
     * @see NodeStatus
     */
    public volatile int status = NodeStatus.FOLLOWER;

    public volatile PeerSet peerSet;

    volatile boolean running = false;

    /* ============ 所有服务器上持久存在的 ============= */

    /** 服务器最后一次知道的任期号（初始化为 0，持续递增） */
    volatile long currentTerm = 0;
    /** 在当前获得选票的候选人的 Id */
    volatile String votedFor;
    /** 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号 */
    LogModule logModule;



    /* ============ 所有服务器上经常变的 ============= */

    /** 已知的最大的已经被提交的日志条目的索引值 */
    volatile long commitIndex;

    /** 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增) */
    volatile long lastApplied = 0;

    /* ========== 在领导人里经常改变的(选举后重新初始化) ================== */

    /** 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一） */
    Map<Peer, Long> nextIndexs;

    public Map<Peer, Long> getNextIndexs() {
        return nextIndexs;
    }

    /** 对于每一个服务器，已经复制给他的日志的最高索引值 */
    Map<Peer, Long> matchIndexs;

    public Map<Peer, Long> getMatchIndexs() {
        return matchIndexs;
    }

    /* ============================== */

    public NodeConfig config;
//client server
    //public RpcService rpcServer;
    public NettyServer rpcServer;

//    public RpcClient rpcClient = new DefaultRpcClient();
    public NettyClient rpcClient = NettyClient.getInstance();
    public StateMachine stateMachine;

    /* ============================== */

    /** 一致性模块实现 */
    Consensus consensus;
    /**
     * 成员变化模块的实现
     */
    ClusterMembershipChanges delegate;


    /* ============================== */

    private DefaultNode() {
    }

    public static DefaultNode getInstance() {
        return DefaultNodeLazyHolder.INSTANCE;
    }


    private static class DefaultNodeLazyHolder {

        private static final DefaultNode INSTANCE = new DefaultNode();
    }

    @Override
    public void init() throws Throwable {
        running = true;
        //SOFARPC
//        rpcServer.init();
//        rpcClient.init();

        //实例化一致性对象
        consensus = new DefaultConsensus(this);
        //实例化节点扩缩容实例对象
        delegate = new ClusterMembershipChangesImpl(this);

        rpcClient.startRpcClient(this);
        //自定义rpc
        //延时启动心跳
        RaftThreadPool.scheduleWithFixedDelay(heartBeatTask, 500);
        //第一次启动的选举任务延时为6S
        RaftThreadPool.scheduleAtFixedRate(electionTask, 6000, 500);
        //启动复制失败处理线程
        RaftThreadPool.execute(replicationFailQueueConsumer);


        LogEntry logEntry = logModule.getLast();
        if (logEntry != null) {
            currentTerm = logEntry.getTerm();//最后一条日志的任期号
        }
        log.info("myId : {} 启动成功", peerSet.getSelf());
        //if(!peerSet.getList().contains(peerSet.getSelf()))


        rpcClient.startRpcClient(this);

        rpcServer.startRpcServer(this);
        //add();//增加节点并同步
    }

    @Override
    public void setConfig(NodeConfig config) {
        this.config = config;
        stateMachine = config.getStateMachineSaveType().getStateMachine();
        logModule = DefaultLogModule.getInstance();

        peerSet = PeerSet.getInstance();
        for (String s : config.getPeerAddrs()) {
            Peer peer = new Peer(s);
            peerSet.addPeer(peer);
        }
        Peer peer = new Peer("localhost:" + config.getSelfPort());
        peerSet.setSelf(peer);
        //rpcServer = new DefaultRpcServiceImpl(config.selfPort, this);
        rpcServer = NettyServer.getInstance();
    }

    /**
     * 处理投票的请求
     *
     * @param param 投票参数
     * @return 投票结果
     */
    @Override
    public RvoteResult handlerRequestVote(RvoteParam param) {
        log.warn("我收到了候选者发来的投票请求 : {}", param);
        return consensus.requestVote(param);
    }
    /**
     * 处理日志追加请求
     */
    @Override
    public AentryResult handlerAppendEntries(AentryParam param) {
        if (param.getEntries() != null) {
            log.warn("节点收到 node {} 追加日志的要求, 日志内容 = {}", param.getLeaderId(), param.getEntries());
        }

        return consensus.appendEntries(param);
    }

    /**
     * 重定向请求给leader
     *
     * @param request 客户端请求
     * @return 响应
     */
    @Override
    public ClientKVAck redirect(ClientKVReq request) throws ExecutionException, InterruptedException, TimeoutException {
        Request r = Request.builder()
                .obj(request)
                .url(peerSet.getLeader().getAddr())
                .cmd(Request.CLIENT_REQ).build();
        Response response=(Response) rpcClient.send(r);
        ClientKVAck clientKVAck=JSON.parseObject(JSON.toJSONString(response.getResult()), ClientKVAck.class);
        return clientKVAck;
    }

    /**
     * 客户端的每一个请求都包含一条被复制状态机执行的指令。
     * 领导人把这条指令作为一条新的日志条目附加到日志中去，然后并行的发起附加条目 RPCs 给其他的服务器，让他们复制这条日志条目。
     * 当这条日志条目被安全的复制（下面会介绍），领导人会应用这条日志条目到它的状态机中然后把执行的结果返回给客户端。
     * 如果跟随者崩溃或者运行缓慢，再或者网络丢包，
     * 领导人会不断的重复尝试附加日志条目 RPCs （尽管已经回复了客户端）直到所有的跟随者都最终存储了所有的日志条目。
     */
    @Override
    public synchronized ClientKVAck handlerClientRequest(ClientKVReq request) throws ExecutionException, InterruptedException, TimeoutException {

        log.warn("处理客户端 {} 操作,  and key : [{}], value : [{}]",
                ClientKVReq.Type.value(request.getType()), request.getKey(), request.getValue());

        if (status != LEADER) {
            log.warn("我不是leader，重定向, leader addr : {}, my addr : {}",
                    peerSet.getLeader(), peerSet.getSelf().getAddr());
            return redirect(request);
        }
//get请求
        if (request.getType() == ClientKVReq.GET) {
            LogEntry logEntry = stateMachine.get(request.getKey());
            if (logEntry != null) {
                return new ClientKVAck(logEntry.getCommand());
            }
            return new ClientKVAck(null);
        }
//处理PUT请求，然后将用户的 KV 数据封装成日志结构，包括 term，index，command，预提交到本地
        LogEntry logEntry = LogEntry.builder()
                .command(Command.builder().
                        key(request.getKey()).
                        value(request.getValue()).
                        build())
                .term(currentTerm)
                .build();

        // 预提交到本地日志, TODO 预提交
        // 如果后面发生日志复制达不到一半的条件，则执行撤销
        logModule.write(logEntry);//key 严格递增的索引值  value logEntry
        log.info("leader的日志预提交到本地, 写入日志信息 : {}, 写入日志的索引 : {}", logEntry, logEntry.getIndex());
        //下面采用原子量进行复制的统计
        final AtomicInteger success = new AtomicInteger(0);
        //异步
        List<Future<Boolean>> futureList = new ArrayList<>();

        int count = 0;
        //  并行的向其他节点发送日志数据，也就是日志复制
        for (Peer peer : peerSet.getPeersWithOutSelf()) {
            // TODO check self and RaftThreadPool
            count++;
            // 并行发起 RPC 日志复制.

            futureList.add(replication(peer, logEntry));
        }
        CountDownLatch latch = new CountDownLatch(futureList.size());
        List<Boolean> resultList = new CopyOnWriteArrayList<>();

        getRPCAppendResult(futureList, latch, resultList);
        try {
            latch.await(4000, MILLISECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        //成功复制的数量
        for (Boolean aBoolean : resultList) {
            if (aBoolean) {
                success.incrementAndGet();
            }
        }

//        // 如果存在一个满足N > commitIndex的 N，并且大多数的matchIndex[i] ≥ N成立，
//        // 并且log[N].term == currentTerm成立，那么令 commitIndex 等于这个 N
//        List<Long> matchIndexList = new ArrayList<>(matchIndexs.values());
//        // 小于 2, 没有意义
//        int median = 0;
//        //System.out.println(matchIndexList);
//        if (matchIndexList.size() >= 2) {
//            Collections.sort(matchIndexList);
//            median = matchIndexList.size() / 2;
//        }
//        Long N = matchIndexList.get(median);
//        if (N > commitIndex) {
//            LogEntry entry = logModule.read(N);
//            System.out.println(entry);
//            if (entry != null && entry.getTerm() == currentTerm) {
//                commitIndex = N;
//            }
//        }

        //  响应客户端(成功一半)
        if (success.get() >= (count / 2)) {

            // 更新
            commitIndex = logEntry.getIndex();
            //  应用到状态机
            getStateMachine().apply(logEntry);
            lastApplied = commitIndex;

            log.info("success apply local state machine,  logEntry info : {}", logEntry);
            // 返回成功.
            return ClientKVAck.ok();
        } else {
            // 回滚已经提交的本地日志.
            logModule.removeOnStartIndex(logEntry.getIndex());
            log.warn("fail apply local state  machine,  logEntry info : {}", logEntry);

            // 这里应该返回错误, 因为没有成功复制过半机器.
            return ClientKVAck.fail();
        }
    }

    private void getRPCAppendResult(List<Future<Boolean>> futureList, CountDownLatch latch, List<Boolean> resultList) {
        for (Future<Boolean> future : futureList) {
            RaftThreadPool.execute(() -> {
                try {
                    resultList.add(future.get(3000, MILLISECONDS));
                } catch (Exception e) {
                    resultList.add(false);
                } finally {
                    latch.countDown();
                }
            });
        }
    }


    /** leader复制日志entry到peer机器 */
    //从上一次复制的位置，到要复制的entry的索引值之间的日志，都要复制，如果上一次复制的位置前一个值一致性检查不对
    //回退上一次复制的位置
    public Future<Boolean> replication(Peer peer, LogEntry entry) {
        return RaftThreadPool.submit(()-> {

            long start = System.currentTimeMillis(), end = start;

            // 20 秒重试时间
            while (end - start < 20 * 1000L) {

                AentryParam aentryParam = AentryParam.builder().build();
                aentryParam.setTerm(currentTerm);//leader的任期
                aentryParam.setServerId(peer.getAddr());//复制给哪台机器
                aentryParam.setLeaderId(peerSet.getSelf().getAddr());//leader的IP

                aentryParam.setLeaderCommit(commitIndex);//领导人已经提交的日志的索引值

                // 每个节点的成功复制过的日志的 index
                Long nextIndex = nextIndexs.get(peer);
                LinkedList<LogEntry> logEntries = new LinkedList<>();

                if (entry.getIndex() >= nextIndex) {//从上一次复制的位置，到要复制的entry的索引值之间的日志都放入集合中
                    for (long i = nextIndex; i <= entry.getIndex(); i++) {
                        LogEntry l = logModule.read(i);
                        if (l != null) {
                            logEntries.add(l);
                        }
                    }
                } else {
                    logEntries.add(entry);//只用复制当前新加入的日志
                }
                // 集合中第一条日志的前一条日志，做一致性检查
                LogEntry preLog = getPreLog(logEntries.getFirst());
                aentryParam.setPreLogTerm(preLog.getTerm());//prevLogIndex 条目的任期号
                aentryParam.setPrevLogIndex(preLog.getIndex());//新的日志条目紧随之前的索引值
                //准备存储的日志条目
                aentryParam.setEntries(logEntries.toArray(new LogEntry[0]));

                Request request = Request.builder()
                        .cmd(Request.A_ENTRIES)
                        .obj(aentryParam)
                        .url(peer.getAddr())
                        .build();

                try {//向follower发出追加日志的请求
                    Response result1 = getRpcClient().send(request);
                    AentryResult result=JSON.parseObject(JSON.toJSONString(result1.getResult()), AentryResult.class);
                    //AentryResult result=(AentryResult)result1.getResult();
                    if (result == null) {
                        log.info("leader向follower增加日志失败");
                        return false;
                    }
                    if (result.isSuccess()) {
                        log.info("leader向follower增加日志成功 , follower=[{}], entry=[{}]", peer, aentryParam.getEntries());
                        // 成功复制到邻接点的位置，更新匹配的位置
                        nextIndexs.put(peer, entry.getIndex() + 1);
                        matchIndexs.put(peer, entry.getIndex());
                        //System.out.println(nextIndexs);
                        return true;
                    } else if (!result.isSuccess()) {
                        // 对方比我大
                        if (result.getTerm() > currentTerm) {
                            log.warn("follower [{}] term [{}] than more self, and my term = [{}], so, I will become follower",
                                    peer, result.getTerm(), currentTerm);
                            currentTerm = result.getTerm();
                            // 认怂, 变成跟随者
                            status = NodeStatus.FOLLOWER;
                            return false;
                        } // 没我大, 却失败了,说明 index 不对.或者 term 不对.
                        else {
                            // 递减
                            if (nextIndex == 0) {
                                nextIndex = 1L;
                            }
                            nextIndexs.put(peer, nextIndex - 1);
                            log.warn("follower {} nextIndex 不匹配, 尝试减小 nextIndex 并重试 RPC append, nextIndex : [{}]", peer.getAddr(),
                                    nextIndex);
                            // 重来, 直到成功.
                        }
                    }

                    end = System.currentTimeMillis();

                } catch (Exception e) {
                    e.printStackTrace();
                    // TODO 不应用到状态机,但已经记录到leader的日志中.由定时任务从重试队列取出,然后重复尝试,当达到条件时,应用到状态机.
//                    log.warn(e.getMessage(), e);
//
//                    ReplicationFailModel model =  ReplicationFailModel.newBuilder()
//                        .callable(this)
//                        .logEntry(entry)
//                        .peer(peer)
//                        .offerTime(System.currentTimeMillis())
//                        .build();
//                    replicationFailQueue.offer(model);
//                    return false;
                }
            }
            // 超时了,没办法了
            return false;
        });

    }

    private LogEntry getPreLog(LogEntry logEntry) {
        LogEntry entry;

        entry = logModule.read(logEntry.getIndex() - 1);

        if (entry == null) {
            log.warn("get perLog is null , parameter logEntry : {}", logEntry);
            entry = LogEntry.builder().index(0L).term(0).command(null).build();
        }
        return entry;
    }


    class ReplicationFailQueueConsumer implements Runnable {

        /** 一分钟 */
        long intervalTime = 1000 * 60;

        @Override
        public void run() {
            while (running) {
                try {
                    ReplicationFailModel model = replicationFailQueue.poll(1000, MILLISECONDS);
                    if (model == null) {
                        continue;
                    }
                    if (status != LEADER) {
                        // 应该清空?
                        replicationFailQueue.clear();
                        continue;
                    }
                    log.warn("将复制失败的日志进行重试, content detail : [{}]", model.logEntry);
                    long offerTime = model.offerTime;
                    if (System.currentTimeMillis() - offerTime > intervalTime) {
                        log.warn("复制失败事件队列可能已满或处理程序速度慢");
                    }

                    Callable callable = model.callable;
                    Future<Boolean> future = RaftThreadPool.submit(callable);
                    Boolean r = future.get(3000, MILLISECONDS);
                    // 重试成功.
                    if (r) {
                        // 可能有资格应用到状态机.
                        tryApplyStateMachine(model);
                    }

                } catch (InterruptedException e) {
                    // ignore
                } catch (ExecutionException | TimeoutException e) {
                    log.warn(e.getMessage());
                }
            }
        }
    }

    private void tryApplyStateMachine(ReplicationFailModel model) {

        String success = stateMachine.getString(model.successKey);
        stateMachine.setString(model.successKey, String.valueOf(Integer.parseInt(success) + 1));

        String count = stateMachine.getString(model.countKey);

        if (Integer.parseInt(success) >= Integer.parseInt(count) / 2) {
            stateMachine.apply(model.logEntry);
            stateMachine.delString(model.countKey, model.successKey);
        }
    }


    @Override
    public void destroy() throws Throwable {
        for (Peer ignore : peerSet.getPeersWithOutSelf()) {
            // TODO 同步到其他节点.
            Request request = Request.builder()
                    .cmd(Request.CHANGE_CONFIG_REMOVE)
                    .url(ignore.getAddr())
                    .obj(peerSet.getSelf())
                    .build();

            Response result1 = rpcClient.send(request);
            Result result=JSON.parseObject(JSON.toJSONString(result1.getResult()), Result.class);
            //Result result=(Result)result1.getResult();
            if (result != null && result.getStatus()== Result.Status.SUCCESS.getCode()) {
                log.info("成功从"+ignore+"删除节点"+peerSet.getSelf()+"成功");
            } else {
                log.info("成功从"+ignore+"删除节点"+peerSet.getSelf()+"失败");
            }
        }

        rpcServer.destroy();
        stateMachine.destroy();
        rpcClient.destroy();
        running = false;
        log.info("destroy success");
    }


    /**
     * 1. 在转变成候选人后就立即开始选举过程
     * 自增当前的任期号（currentTerm）
     * 给自己投票
     * 重置选举超时计时器
     * 发送请求投票的 RPC 给其他所有服务器
     * 2. 如果接收到大多数服务器的选票，那么就变成领导人
     * 3. 如果接收到来自新的领导人的附加日志 RPC，转变成跟随者
     * 4. 如果选举过程超时，再次发起一轮选举
     */
    class ElectionTask implements Runnable {

        @Override
        public void run() {

            if (status == LEADER) {
                return;
            }

            long current = System.currentTimeMillis();
            // 基于 RAFT 的随机时间,解决冲突.
            electionTime = electionTime + ThreadLocalRandom.current().nextInt(50);
            if (current - preElectionTime < electionTime) {
                return;
            }
            status = NodeStatus.CANDIDATE;
            log.error("node {} 变成 CANDIDATE 开始竞选leader, 当前任期号 : [{}], 我的最后一条日志 : [{}]",
                    peerSet.getSelf(), currentTerm, logModule.getLast());

            preElectionTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(200) + 150;

            currentTerm = currentTerm + 1;
            // 推荐自己.
            votedFor = peerSet.getSelf().getAddr();

            List<Peer> peers = peerSet.getPeersWithOutSelf();

            ArrayList<Future<RvoteResult>> futureArrayList = new ArrayList<>();

            log.info("可以找到的同伴数量 : {}, 同伴内容 : {}", peers.size(), peers);
            // 发送请求
            for (Peer peer : peers) {

                futureArrayList.add(RaftThreadPool.submit(() -> {
                    long lastTerm = 0L;
                    LogEntry last = logModule.getLast();
                    if (last != null) {
                        lastTerm = last.getTerm();
                    }

                    RvoteParam param = RvoteParam.builder().
                            term(currentTerm).//候选人的任期号
                            candidateId(peerSet.getSelf().getAddr()).
                            lastLogIndex(LongConvert.convert(logModule.getLastIndex())).//最后一条日志的索引
                            lastLogTerm(lastTerm).//最后一条日志的任期号
                            build();

                    Request request = Request.builder()
                            .cmd(Request.R_VOTE)//请求类型
                            .obj(param)
                            .url(peer.getAddr())
                            .build();

                    try {
                        Response<RvoteResult> response = getRpcClient().send(request);
                        return response;
                    } catch (RaftRemotingException e) {
                        e.printStackTrace();
                        log.error("选举任务 RPC 处理失败 , URL : " + request.getUrl());
                        return null;
                    }
                }));
            }

            AtomicInteger success2 = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(futureArrayList.size());


            // 等待结果.
            for (Future future : futureArrayList) {
                RaftThreadPool.submit(() -> {
                    try {
                        @SuppressWarnings("unchecked")
                        Response<RvoteResult> response = (Response<RvoteResult>) future.get(3000, MILLISECONDS);
                        if (response == null) {
                            return -1;
                        }
                        RvoteResult result=JSON.parseObject(JSON.toJSONString(response.getResult()), RvoteResult.class);
                        boolean isVoteGranted = result.isVoteGranted();


                        if (isVoteGranted) {
                            success2.incrementAndGet();
                        } else {
                            // 更新自己的任期.
                            //如果一个服务器的当前任期号比其他的小，该服务器会将自己的任期号更新为较大的那个值。
                            long resTerm = result.getTerm();
                            if (resTerm >= currentTerm) {
                                currentTerm = resTerm;
                            }
                        }
                        return 0;
                    } catch (Exception e) {
                        log.error("future.get exception , e : ", e);
                        return -1;
                    } finally {
                        latch.countDown();
                    }
                });
            }

            try {
                //等待投票结果应该有超时控制，如果超时了，就不等待了
                latch.await(3500, MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("InterruptedException By Master election Task");
            }

            int success = success2.get();
            log.info("node {} 统计竞选结果 , 获得同伴票数 = {} , 此时状态 : {}", peerSet.getSelf(), success, NodeStatus.Enum.value(status));
            // 如果投票期间,有其他服务器发送 appendEntry , 就可能变成 follower ,这时,应该停止.
            if (status == NodeStatus.FOLLOWER) {
                return;
            }
            /// 加上自身.
            if (success+1 > (peers.size()+1) / 2) {
                log.warn("node {} 成功当选 leader ", peerSet.getSelf());
                status = LEADER;
                peerSet.setLeader(peerSet.getSelf());
                votedFor = "";
                becomeLeaderToDoThing();
            } else {
                // else 重新选举
                votedFor = "";
            }

        }
    }

    /**
     * 初始化所有的 nextIndex 值为自己的最后一条日志的 index + 1. 如果下次 RPC 时, 跟随者和leader 不一致,就会失败.
     * 那么 leader 尝试递减 nextIndex 并进行重试.最终将达成一致.
     */
    private void becomeLeaderToDoThing() {
        nextIndexs = new ConcurrentHashMap<>();
        matchIndexs = new ConcurrentHashMap<>();
        //每个节点竞选leader后，会统计每个同伴的最后一条日志索引（即已提交）
        for (Peer peer : peerSet.getPeersWithOutSelf()) {
            nextIndexs.put(peer, logModule.getLastIndex() + 1);
            matchIndexs.put(peer, 0L);
        }
    }

    ///日志复制有 2 种形式，1种是心跳，一种是真正的日志，心跳的日志内容是空的，其他部分基本相同，
// 也就是说，接收方在收到日志时，如果发现是空的，那么他就是心跳
    class HeartBeatTask implements Runnable {

        @Override
        public void run() {

            if (status != LEADER) {
                return;
            }
            long current = System.currentTimeMillis();
            if (current - preHeartBeatTime < heartBeatTick) {
                return;
            }
            log.info("=========== 向节点发送心跳 =============");
            for (Peer peer : peerSet.getPeersWithOutSelf()) {
                log.info("向节点 {} 发送心跳", peer.getAddr());
            }

            preHeartBeatTime = System.currentTimeMillis();


            for (Peer peer : peerSet.getPeersWithOutSelf()) {

                AentryParam param = AentryParam.builder()
                        .entries(null)// 心跳,空日志.
                        .leaderId(peerSet.getSelf().getAddr())
                        .serverId(peer.getAddr())//同伴IP
                        .term(currentTerm)//leader的任期
                        .build();

                Request request = new Request(
                        Request.A_ENTRIES,
                        param,
                        peer.getAddr());

                RaftThreadPool.execute(() -> {
                    try {
                        Response response = getRpcClient().send(request);
                        AentryResult AppendLogResult=JSON.parseObject(JSON.toJSONString(response.getResult()), AentryResult.class);
                        //AentryResult AppendLogResult = (AentryResult) response.getResult();
                        long term = AppendLogResult.getTerm();
//如果任意 follower 的返回值的 term 大于自身，说明自己分区了（我是挂了又恢复再发送的心跳包），为防止脑裂，那么需要变成 follower，
// 并更新自己的 term。然后重新发起选举
                        if (term > currentTerm) {
                            log.error("避免脑裂现象，变成 follower, 领导者任期 : {}, 当前任期 : {}", term, currentTerm);
                            currentTerm = term;
                            votedFor = "";
                            status = NodeStatus.FOLLOWER;
                        }
                    } catch (Exception e) {
                        log.error("HeartBeatTask RPC Fail, request URL : {} ", request.getUrl());
                    }
                }, false);
            }
        }
    }

    @Override
    public Result addPeer(Peer newPeer) throws ExecutionException, InterruptedException, TimeoutException {
        return delegate.addPeer(newPeer);
    }
    public Result addPeerSet(PeerSet newPeerSet) {
        return delegate.addPeerSet(newPeerSet);
    }
    @Override
    public Result removePeer(Peer oldPeer) {
        return delegate.removePeer(oldPeer);
    }

    public void add() throws ExecutionException, InterruptedException, TimeoutException {//把要增加的节点同步给所有的peerset，从一个peerset中找leader节点，向leader节点同步，然后leader节点再下发到所有节点
        for (Peer ignore : peerSet.getPeersWithOutSelf()) {
            // TODO 同步到其他节点.
            Request request = Request.builder()
                    .cmd(Request.CHANGE_CONFIG_ADD)
                    .url(ignore.getAddr())
                    .obj(peerSet.getSelf())
                    .build();

            Response result1 = rpcClient.send(request);
            Result result;
            if(result1!=null)
                result=JSON.parseObject(JSON.toJSONString(result1.getResult()), Result.class);
            else{
                result =null;
            }
            if (result != null && result.getStatus() == Result.Status.SUCCESS.getCode()) {
                log.info("向"+ignore.getAddr()+"增加节点" + peerSet.getSelf() + "成功");
            } else {
                log.info("向"+ignore.getAddr()+"增加节点" + peerSet.getSelf() + "失败");
            }
        }
    }
}
