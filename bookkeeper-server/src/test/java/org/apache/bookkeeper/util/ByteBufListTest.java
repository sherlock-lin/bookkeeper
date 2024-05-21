/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import java.net.SocketAddress;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ByteBufList}.
 */
class ByteBufListTest {
    @Test
    void single() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBufList buf = ByteBufList.get(b1);

        assertEquals(1, buf.size());
        assertEquals(128, buf.readableBytes());
        assertEquals(b1, buf.getBuffer(0));

        assertEquals(1, buf.refCnt());
        assertEquals(1, b1.refCnt());

        buf.release();

        assertEquals(0, buf.refCnt());
        assertEquals(0, b1.refCnt());
    }

    @Test
    void testDouble() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b2.writerIndex(b2.capacity());
        ByteBufList buf = ByteBufList.get(b1, b2);

        assertEquals(2, buf.size());
        assertEquals(256, buf.readableBytes());
        assertEquals(b1, buf.getBuffer(0));
        assertEquals(b2, buf.getBuffer(1));

        assertEquals(1, buf.refCnt());
        assertEquals(1, b1.refCnt());
        assertEquals(1, b2.refCnt());

        buf.release();

        assertEquals(0, buf.refCnt());
        assertEquals(0, b1.refCnt());
        assertEquals(0, b2.refCnt());
    }

    @Test
    void testClone() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b2.writerIndex(b2.capacity());
        ByteBufList buf = ByteBufList.get(b1, b2);

        ByteBufList clone = ByteBufList.clone(buf);

        assertEquals(2, buf.size());
        assertEquals(256, buf.readableBytes());
        assertEquals(b1, buf.getBuffer(0));
        assertEquals(b2, buf.getBuffer(1));

        assertEquals(2, clone.size());
        assertEquals(256, clone.readableBytes());
        assertEquals(b1, clone.getBuffer(0));
        assertEquals(b2, clone.getBuffer(1));

        assertEquals(1, buf.refCnt());
        assertEquals(1, clone.refCnt());
        assertEquals(2, b1.refCnt());
        assertEquals(2, b2.refCnt());

        buf.release();

        assertEquals(0, buf.refCnt());
        assertEquals(1, clone.refCnt());
        assertEquals(1, b1.refCnt());
        assertEquals(1, b2.refCnt());

        clone.release();

        assertEquals(0, buf.refCnt());
        assertEquals(0, clone.refCnt());
        assertEquals(0, b1.refCnt());
        assertEquals(0, b2.refCnt());
    }

    @Test
    void getBytes() throws Exception {
        ByteBufList buf = ByteBufList.get(Unpooled.wrappedBuffer("hello".getBytes()),
                Unpooled.wrappedBuffer("world".getBytes()));

        assertArrayEquals("helloworld".getBytes(), buf.toArray());

        buf.prepend(Unpooled.wrappedBuffer("prefix-".getBytes()));
        assertArrayEquals("prefix-helloworld".getBytes(), buf.toArray());

        // Bigger buffer
        byte[] buf100 = new byte[100];
        int res = buf.getBytes(buf100);

        assertEquals("prefix-helloworld".length(), res);

        // Smaller buffer
        byte[] buf4 = new byte[4];
        res = buf.getBytes(buf4);

        assertEquals(4, res);
        assertEquals("pref", new String(buf4));
    }

    @Test
    void coalesce() throws Exception {
        ByteBufList buf = ByteBufList.get(Unpooled.wrappedBuffer("hello".getBytes()),
                Unpooled.wrappedBuffer("world".getBytes()));

        assertEquals(Unpooled.wrappedBuffer("helloworld".getBytes()), ByteBufList.coalesce(buf));
    }

    @Test
    void retain() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBufList buf = ByteBufList.get(b1);

        assertEquals(1, buf.size());
        assertEquals(128, buf.readableBytes());
        assertEquals(b1, buf.getBuffer(0));

        assertEquals(1, buf.refCnt());
        assertEquals(1, b1.refCnt());

        buf.retain();

        assertEquals(2, buf.refCnt());
        assertEquals(1, b1.refCnt());

        buf.release();

        assertEquals(1, buf.refCnt());
        assertEquals(1, b1.refCnt());

        buf.release();

        assertEquals(0, buf.refCnt());
        assertEquals(0, b1.refCnt());
    }

    @Test
    void encoder() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b2.writerIndex(b2.capacity());
        ByteBufList buf = ByteBufList.get(b1, b2);

        ChannelHandlerContext ctx = new MockChannelHandlerContext();

        ByteBufList.ENCODER.write(ctx, buf, null);

        assertEquals(0, buf.refCnt());
        assertEquals(0, b1.refCnt());
        assertEquals(0, b2.refCnt());
    }

    class MockChannelHandlerContext implements ChannelHandlerContext {
        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture disconnect() {
            return null;
        }

        @Override
        public ChannelFuture close() {
            return null;
        }

        @Override
        public ChannelFuture deregister() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture disconnect(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture close(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture deregister(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture write(Object msg) {
            ReferenceCountUtil.release(msg);
            return null;
        }

        @Override
        public ChannelFuture write(Object msg, ChannelPromise promise) {
            ReferenceCountUtil.release(msg);
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            ReferenceCountUtil.release(msg);
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            ReferenceCountUtil.release(msg);
            return null;
        }

        @Override
        public ChannelPromise newPromise() {
            return null;
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise() {
            return null;
        }

        @Override
        public ChannelFuture newSucceededFuture() {
            return null;
        }

        @Override
        public ChannelFuture newFailedFuture(Throwable cause) {
            return null;
        }

        @Override
        public ChannelPromise voidPromise() {
            return null;
        }

        @Override
        public Channel channel() {
            return null;
        }

        @Override
        public EventExecutor executor() {
            return null;
        }

        @Override
        public String name() {
            return null;
        }

        @Override
        public ChannelHandler handler() {
            return null;
        }

        @Override
        public boolean isRemoved() {
            return false;
        }

        @Override
        public ChannelHandlerContext fireChannelRegistered() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelUnregistered() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelActive() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelInactive() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireUserEventTriggered(Object evt) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelRead(Object msg) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelReadComplete() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelWritabilityChanged() {
            return null;
        }

        @Override
        public ChannelHandlerContext read() {
            return null;
        }

        @Override
        public ChannelHandlerContext flush() {
            return null;
        }

        @Override
        public ChannelPipeline pipeline() {
            return null;
        }

        @Override
        public ByteBufAllocator alloc() {
            return null;
        }

        @Override
        @Deprecated
        public <T> Attribute<T> attr(AttributeKey<T> key) {
            return null;
        }

        @Override
        @Deprecated
        public <T> boolean hasAttr(AttributeKey<T> key) {
            return false;
        }

    }
}
