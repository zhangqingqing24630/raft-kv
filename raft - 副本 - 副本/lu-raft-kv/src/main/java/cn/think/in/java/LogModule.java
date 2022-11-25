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
package cn.think.in.java;

import cn.think.in.java.entity.LogEntry;

/**
 日志模块的接口定义
 */
public interface LogModule extends LifeCycle {
    /**
     * 写日志
     */
    void write(LogEntry logEntry);
    /**
     * 读日志
     */
    LogEntry read(Long index);
    /**
     * 移出从offset开始的日志
     */
    void removeOnStartIndex(Long startIndex);
    /**
     * 获取最后的日志
     */
    LogEntry getLast();
    /**
     * 获取最后的日志索引
     */
    Long getLastIndex();
}
