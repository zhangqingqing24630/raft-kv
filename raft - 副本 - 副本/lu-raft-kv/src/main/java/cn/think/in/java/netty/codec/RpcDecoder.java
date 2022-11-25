package cn.think.in.java.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class RpcDecoder extends ByteToMessageDecoder {

    private Class<?> clazz;
    private Serializer serializer;

    public RpcDecoder(Class<?> clazz, Serializer serializer) {
        this.clazz = clazz;
        this.serializer = serializer;
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        // Check if there are at least 4 bytes readable
        if (byteBuf.readableBytes() >= 4) {
            int readInt = byteBuf.readInt();
            byte[] bytes = new byte[readInt];
            byteBuf.readBytes(bytes);
            Object deserialize = serializer.deserialize(clazz, bytes);
            list.add(deserialize);
        }
    }
}
