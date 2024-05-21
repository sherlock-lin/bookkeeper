/*
 *
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
 *
 */
package org.apache.bookkeeper.proto;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest.Flag;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacRequest;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.jupiter.api.Test;

/**
 * Test utility methods from bookie request processor.
 */
class TestBookieRequestProcessor {

    final BookieRequestProcessor requestProcessor = mock(BookieRequestProcessor.class);

    private final ChannelGroup channelGroup = new DefaultChannelGroup(null);

    @Test
    void constructLongPollThreads() throws Exception {
        // long poll threads == read threads
        ServerConfiguration conf = new ServerConfiguration();
        try (BookieRequestProcessor processor = new BookieRequestProcessor(
            conf, mock(Bookie.class), NullStatsLogger.INSTANCE, null, UnpooledByteBufAllocator.DEFAULT,
                channelGroup)) {
            assertSame(processor.getReadThreadPool(), processor.getLongPollThreadPool());
        }

        // force create long poll threads if there is no read threads
        conf = new ServerConfiguration();
        conf.setNumReadWorkerThreads(0);
        try (BookieRequestProcessor processor = new BookieRequestProcessor(
            conf, mock(Bookie.class), NullStatsLogger.INSTANCE, null, UnpooledByteBufAllocator.DEFAULT,
                channelGroup)) {
            assertNull(processor.getReadThreadPool());
            assertNotNull(processor.getLongPollThreadPool());
        }

        // long poll threads and no read threads
        conf = new ServerConfiguration();
        conf.setNumReadWorkerThreads(2);
        conf.setNumLongPollWorkerThreads(2);
        try (BookieRequestProcessor processor = new BookieRequestProcessor(
            conf, mock(Bookie.class), NullStatsLogger.INSTANCE, null, UnpooledByteBufAllocator.DEFAULT,
                channelGroup)) {
            assertNotNull(processor.getReadThreadPool());
            assertNotNull(processor.getLongPollThreadPool());
            assertNotSame(processor.getReadThreadPool(), processor.getLongPollThreadPool());
        }
    }

    @Test
    void flagsV3() {
        ReadRequest read = ReadRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setFlag(ReadRequest.Flag.FENCE_LEDGER).build();
        assertTrue(RequestUtils.hasFlag(read, ReadRequest.Flag.FENCE_LEDGER));
        assertFalse(RequestUtils.hasFlag(read, ReadRequest.Flag.ENTRY_PIGGYBACK));

        read = ReadRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setFlag(ReadRequest.Flag.ENTRY_PIGGYBACK).build();
        assertFalse(RequestUtils.hasFlag(read, ReadRequest.Flag.FENCE_LEDGER));
        assertTrue(RequestUtils.hasFlag(read, ReadRequest.Flag.ENTRY_PIGGYBACK));

        read = ReadRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .build();
        assertFalse(RequestUtils.hasFlag(read, ReadRequest.Flag.FENCE_LEDGER));
        assertFalse(RequestUtils.hasFlag(read, ReadRequest.Flag.ENTRY_PIGGYBACK));

        AddRequest add = AddRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setFlag(AddRequest.Flag.RECOVERY_ADD)
            .setMasterKey(ByteString.EMPTY)
            .setBody(ByteString.EMPTY)
            .build();
        assertTrue(RequestUtils.hasFlag(add, AddRequest.Flag.RECOVERY_ADD));

        add = AddRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setMasterKey(ByteString.EMPTY)
            .setBody(ByteString.EMPTY)
            .build();
        assertFalse(RequestUtils.hasFlag(add, AddRequest.Flag.RECOVERY_ADD));

        add = AddRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setFlag(AddRequest.Flag.RECOVERY_ADD)
            .setMasterKey(ByteString.EMPTY)
            .setBody(ByteString.EMPTY)
            .build();
        assertTrue(RequestUtils.hasFlag(add, AddRequest.Flag.RECOVERY_ADD));
    }

    @Test
    void testToString() {
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder();
        headerBuilder.setVersion(ProtocolVersion.VERSION_THREE);
        headerBuilder.setOperation(OperationType.ADD_ENTRY);
        headerBuilder.setTxnId(5L);
        BKPacketHeader header = headerBuilder.build();

        AddRequest addRequest = AddRequest.newBuilder().setLedgerId(10).setEntryId(1)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes()))
                .setBody(ByteString.copyFrom("entrydata".getBytes())).build();
        Request request = Request.newBuilder().setHeader(header).setAddRequest(addRequest).build();

        Channel channel = mock(Channel.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        BookieRequestHandler requestHandler = mock(BookieRequestHandler.class);
        when(requestHandler.ctx()).thenReturn(ctx);

        WriteEntryProcessorV3 writeEntryProcessorV3 = new WriteEntryProcessorV3(request, requestHandler,
                requestProcessor);
        String toString = writeEntryProcessorV3.toString();
        assertFalse(toString.contains("body"), "writeEntryProcessorV3's toString should have filtered out body");
        assertFalse(toString.contains("masterKey"),
                "writeEntryProcessorV3's toString should have filtered out masterKey");
        assertTrue(toString.contains("ledgerId"), "writeEntryProcessorV3's toString should contain ledgerId");
        assertTrue(toString.contains("entryId"), "writeEntryProcessorV3's toString should contain entryId");
        assertTrue(toString.contains("version"), "writeEntryProcessorV3's toString should contain version");
        assertTrue(toString.contains("operation"), "writeEntryProcessorV3's toString should contain operation");
        assertTrue(toString.contains("txnId"), "writeEntryProcessorV3's toString should contain txnId");
        assertFalse(toString.contains("flag"), "writeEntryProcessorV3's toString shouldn't contain flag");
        assertFalse(toString.contains("writeFlags"), "writeEntryProcessorV3's toString shouldn't contain writeFlags");

        addRequest = AddRequest.newBuilder().setLedgerId(10).setEntryId(1)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes()))
                .setBody(ByteString.copyFrom("entrydata".getBytes())).setFlag(Flag.RECOVERY_ADD).setWriteFlags(0)
                .build();
        request = Request.newBuilder().setHeader(header).setAddRequest(addRequest).build();
        writeEntryProcessorV3 = new WriteEntryProcessorV3(request, requestHandler, requestProcessor);
        toString = writeEntryProcessorV3.toString();
        assertFalse(toString.contains("body"), "writeEntryProcessorV3's toString should have filtered out body");
        assertFalse(toString.contains("masterKey"),
                "writeEntryProcessorV3's toString should have filtered out masterKey");
        assertTrue(toString.contains("flag"), "writeEntryProcessorV3's toString should contain flag");
        assertTrue(toString.contains("writeFlags"), "writeEntryProcessorV3's toString should contain writeFlags");

        ReadRequest readRequest = ReadRequest.newBuilder().setLedgerId(10).setEntryId(23)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes())).build();
        request = Request.newBuilder().setHeader(header).setReadRequest(readRequest).build();
        toString = RequestUtils.toSafeString(request);
        assertFalse(toString.contains("masterKey"), "ReadRequest's safeString should have filtered out masterKey");
        assertTrue(toString.contains("ledgerId"), "ReadRequest's safeString should contain ledgerId");
        assertTrue(toString.contains("entryId"), "ReadRequest's safeString should contain entryId");
        assertTrue(toString.contains("version"), "ReadRequest's safeString should contain version");
        assertTrue(toString.contains("operation"), "ReadRequest's safeString should contain operation");
        assertTrue(toString.contains("txnId"), "ReadRequest's safeString should contain txnId");
        assertFalse(toString.contains("flag"), "ReadRequest's safeString shouldn't contain flag");
        assertFalse(toString.contains("previousLAC"), "ReadRequest's safeString shouldn't contain previousLAC");
        assertFalse(toString.contains("timeOut"), "ReadRequest's safeString shouldn't contain timeOut");

        readRequest = ReadRequest.newBuilder().setLedgerId(10).setEntryId(23).setPreviousLAC(2).setTimeOut(100)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes())).setFlag(ReadRequest.Flag.ENTRY_PIGGYBACK)
                .build();
        request = Request.newBuilder().setHeader(header).setReadRequest(readRequest).build();
        toString = RequestUtils.toSafeString(request);
        assertFalse(toString.contains("masterKey"), "ReadRequest's safeString should have filtered out masterKey");
        assertTrue(toString.contains("flag"), "ReadRequest's safeString shouldn contain flag");
        assertTrue(toString.contains("previousLAC"), "ReadRequest's safeString shouldn contain previousLAC");
        assertTrue(toString.contains("timeOut"), "ReadRequest's safeString shouldn contain timeOut");

        WriteLacRequest writeLacRequest = WriteLacRequest.newBuilder().setLedgerId(10).setLac(23)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes()))
                .setBody(ByteString.copyFrom("entrydata".getBytes())).build();
        request = Request.newBuilder().setHeader(header).setWriteLacRequest(writeLacRequest).build();
        WriteLacProcessorV3 writeLacProcessorV3 = new WriteLacProcessorV3(request, null, requestProcessor);
        toString = writeLacProcessorV3.toString();
        assertFalse(toString.contains("body"), "writeLacProcessorV3's toString should have filtered out body");
        assertFalse(toString.contains("masterKey"),
                "writeLacProcessorV3's toString should have filtered out masterKey");
        assertTrue(toString.contains("ledgerId"), "writeLacProcessorV3's toString should contain ledgerId");
        assertTrue(toString.contains("lac"), "writeLacProcessorV3's toString should contain lac");
        assertTrue(toString.contains("version"), "writeLacProcessorV3's toString should contain version");
        assertTrue(toString.contains("operation"), "writeLacProcessorV3's toString should contain operation");
        assertTrue(toString.contains("txnId"), "writeLacProcessorV3's toString should contain txnId");
    }
}
