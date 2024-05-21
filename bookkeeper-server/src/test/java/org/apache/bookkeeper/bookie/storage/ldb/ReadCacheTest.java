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
package org.apache.bookkeeper.bookie.storage.ldb;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link ReadCache}.
 */
class ReadCacheTest {

    @Test
    void simple() {
        ReadCache cache = new ReadCache(UnpooledByteBufAllocator.DEFAULT, 10 * 1024);

        assertEquals(0, cache.count());
        assertEquals(0, cache.size());

        ByteBuf entry = Unpooled.wrappedBuffer(new byte[1024]);
        cache.put(1, 0, entry);

        assertEquals(1, cache.count());
        assertEquals(1024, cache.size());

        assertEquals(entry, cache.get(1, 0));
        assertNull(cache.get(1, 1));

        for (int i = 1; i < 10; i++) {
            cache.put(1, i, entry);
        }

        assertEquals(10, cache.count());
        assertEquals(10 * 1024, cache.size());

        cache.put(1, 10, entry);

        // First half of entries will have been evicted
        for (int i = 0; i < 5; i++) {
            assertNull(cache.get(1, i));
        }

        for (int i = 5; i < 11; i++) {
            assertEquals(entry, cache.get(1, i));
        }

        cache.close();
    }

    @Test
    void emptyCache() {
        ReadCache cache = new ReadCache(UnpooledByteBufAllocator.DEFAULT, 10 * 1024);

        assertEquals(0, cache.count());
        assertEquals(0, cache.size());
        assertNull(cache.get(0, 0));

        cache.close();
    }

    @Test
    void multipleSegments() {
        // Test with multiple smaller segments
        ReadCache cache = new ReadCache(UnpooledByteBufAllocator.DEFAULT, 10 * 1024, 2 * 1024);

        assertEquals(0, cache.count());
        assertEquals(0, cache.size());

        for (int i = 0; i < 10; i++) {
            ByteBuf entry = Unpooled.wrappedBuffer(new byte[1024]);
            entry.setInt(0, i);
            cache.put(1, i, entry);
        }

        for (int i = 0; i < 10; i++) {
            ByteBuf res = cache.get(1, i);
            assertEquals(1, res.refCnt());

            assertEquals(1024, res.readableBytes());
            assertEquals(i, res.getInt(0));
        }

        assertEquals(10, cache.count());
        assertEquals(10 * 1024, cache.size());

        // Putting one more entry, should trigger the 1st segment rollover
        ByteBuf entry = Unpooled.wrappedBuffer(new byte[1024]);
        cache.put(2, 0, entry);

        assertEquals(9, cache.count());
        assertEquals(9 * 1024, cache.size());

        cache.close();
    }

    @Test
    void hasEntry() {
        ReadCache cache = new ReadCache(UnpooledByteBufAllocator.DEFAULT, 10 * 1024, 2 * 1024);

        long ledgerId = 0xfefe;
        for (int i = 0; i < 10; i++) {
            ByteBuf entry = Unpooled.wrappedBuffer(new byte[1024]);
            entry.setInt(0, i);
            cache.put(ledgerId, i, entry);
        }

        assertFalse(cache.hasEntry(0xdead, 0));
        assertFalse(cache.hasEntry(ledgerId, -1));
        for (int i = 0; i < 10; i++) {
            assertTrue(cache.hasEntry(ledgerId, i));
        }
        assertFalse(cache.hasEntry(ledgerId, 10));
    }

}
