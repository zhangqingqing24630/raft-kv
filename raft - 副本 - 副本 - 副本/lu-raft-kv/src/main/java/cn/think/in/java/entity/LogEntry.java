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

import cn.think.in.java.LogModule;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Objects;

//@NoArgsConstructor
//@AllArgsConstructor
@Data
@Builder
public class LogEntry implements Serializable, Comparable {

    /**
     * 日志索引
     */
    private Long index;
    /**
     * 日志任期号码
     */
    private long term;
    /**
     * K-V命令,该命令不用于一致性同步，而是用于键值存储
     * @see  Command
     */
    private Command command;

    public LogEntry() {
    }

    public LogEntry(long term, Command command) {
        this.term = term;
        this.command = command;
    }

    public LogEntry(Long index, long term, Command command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }

    private LogEntry(Builder builder) {
        setIndex(builder.index);
        setTerm(builder.term);
        setCommand(builder.command);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "{" +
                "index=" + index +
                ", term=" + term +
                ", command=" + command +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        if (o == null) {
            return -1;
        }
        if (this.getIndex() > ((LogEntry) o).getIndex()) {
            return 1;
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogEntry logEntry = (LogEntry) o;
        return term == logEntry.term &&
                Objects.equals(index, logEntry.index) &&
                Objects.equals(command, logEntry.command);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, term, command);
    }

    public static final class Builder {

        private Long index;
        private long term;
        private Command command;

        private Builder() {
        }

        public Builder index(Long val) {
            index = val;
            return this;
        }

        public Builder term(long val) {
            term = val;
            return this;
        }

        public Builder command(Command val) {
            command = val;
            return this;
        }

        public LogEntry build() {
            return new LogEntry(this);
        }
    }

    public Long getIndex() {
        return index;
    }

    public void setIndex(Long index) {
        this.index = index;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
    }
}
