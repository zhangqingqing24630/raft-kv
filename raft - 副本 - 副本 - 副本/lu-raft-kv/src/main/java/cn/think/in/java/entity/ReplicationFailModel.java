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
package cn.think.in.java.entity;

import cn.think.in.java.common.Peer;
import lombok.Builder;

import java.util.concurrent.Callable;


public class ReplicationFailModel {
    static String count = "_count";
    static String success = "_success";

    public String countKey;
    public String successKey;
    public Callable callable;
    public LogEntry logEntry;
    public Peer peer;
    public Long offerTime;

    public ReplicationFailModel(Callable callable, LogEntry logEntry, Peer peer, Long offerTime) {
        this.callable = callable;
        this.logEntry = logEntry;
        this.peer = peer;
        this.offerTime = offerTime;
        countKey = logEntry.getCommand().getKey() + count;
        successKey = logEntry.getCommand().getKey() + success;
    }

    private ReplicationFailModel(Builder builder) {
        countKey = builder.countKey;
        successKey = builder.successKey;
        callable = builder.callable;
        logEntry = builder.logEntry;
        peer = builder.peer;
        offerTime = builder.offerTime;
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static final class Builder {

        private String countKey;
        private String successKey;
        private Callable callable;
        private LogEntry logEntry;
        private Peer peer;
        private Long offerTime;

        private Builder() {
        }

        public Builder countKey(String val) {
            countKey = val;
            return this;
        }

        public Builder successKey(String val) {
            successKey = val;
            return this;
        }

        public Builder callable(Callable val) {
            callable = val;
            return this;
        }

        public Builder logEntry(LogEntry val) {
            logEntry = val;
            return this;
        }

        public Builder peer(Peer val) {
            peer = val;
            return this;
        }

        public Builder offerTime(Long val) {
            offerTime = val;
            return this;
        }

        public ReplicationFailModel build() {
            return new ReplicationFailModel(this);
        }
    }
}
