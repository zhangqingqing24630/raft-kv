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

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Objects;

/**
 * 命令实体，每个命令实体由键值对构成。
 */
@Getter
@Setter
@ToString
@Builder
public class Command implements Serializable {
    /**
     * 序列化后的键
     */
    private String key;
    /**
     * 序列化后的值
     */
    private String value;

    public Command(String key, String value) {
        this.key = key;
        this.value = value;
    }

    private Command(Builder builder) {
        setKey(builder.key);
        setValue(builder.value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Command command = (Command) o;
        return Objects.equals(key, command.key) &&
                Objects.equals(value, command.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private String key;
        private String value;

        private Builder() {
        }

        public Builder key(String val) {
            key = val;
            return this;
        }

        public Builder value(String val) {
            value = val;
            return this;
        }

        public Command build() {
            return new Command(this);
        }
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

