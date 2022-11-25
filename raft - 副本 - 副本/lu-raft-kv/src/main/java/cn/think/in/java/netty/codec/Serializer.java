package cn.think.in.java.netty.codec;

import java.io.IOException;

public interface Serializer {
    /**
     * java对象转换为二进制
     */
    byte[] serialize(Object object) throws IOException;

    /**
     * 二进制转换成java对象
     */
    <T> T deserialize(Class<T> clazz, byte[] bytes) throws IOException;
}