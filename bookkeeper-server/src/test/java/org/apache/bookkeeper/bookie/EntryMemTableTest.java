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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.PrimitiveIterator.OfLong;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test the EntryMemTable class.
 */
@Slf4j
public class EntryMemTableTest implements CacheCallback, SkipListFlusher, CheckpointSource {

    private Class entryMemTableClass;
    private EntryMemTable memTable;
    private final Random random = new Random();
    private TestCheckPoint curCheckpoint = new TestCheckPoint(0, 0);

    public static Collection<Object[]> memTableClass() {
        return Arrays.asList(new Object[][] { { EntryMemTable.class }, { EntryMemTableWithParallelFlusher.class } });
    }

    public void initEntryMemTableTest(Class entryMemTableClass) {
        this.entryMemTableClass = entryMemTableClass;
    }

    @Override
    public Checkpoint newCheckpoint() {
        return curCheckpoint;
    }

    @Override
    public void checkpointComplete(Checkpoint checkpoint, boolean compact)
            throws IOException {
    }

    @BeforeEach
    void setUp() throws Exception {
        if (entryMemTableClass.equals(EntryMemTableWithParallelFlusher.class)) {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            this.memTable = new EntryMemTableWithParallelFlusher(conf, this, NullStatsLogger.INSTANCE);
        } else {
            this.memTable = new EntryMemTable(TestBKConfiguration.newServerConfiguration(), this,
                    NullStatsLogger.INSTANCE);
        }
    }

    @AfterEach
    void cleanup() throws Exception{
        this.memTable.close();
    }

    @MethodSource("memTableClass")
    @ParameterizedTest
    public void logMark(Class entryMemTableClass) throws IOException {
        initEntryMemTableTest(entryMemTableClass);
        LogMark mark = new LogMark();
        assertEquals(0, mark.compare(new LogMark()));
        assertTrue(mark.compare(LogMark.MAX_VALUE) < 0);
        mark.setLogMark(3, 11);
        byte[] data = new byte[16];
        ByteBuffer buf = ByteBuffer.wrap(data);
        mark.writeLogMark(buf);
        buf.flip();
        LogMark mark1 = new LogMark(9, 13);
        assertTrue(mark1.compare(mark) > 0);
        mark1.readLogMark(buf);
        assertEquals(0, mark1.compare(mark));
    }

    /**
     * Basic put/get.
     * @throws IOException
     * */
    @MethodSource("memTableClass")
    @ParameterizedTest
    public void basicOps(Class entryMemTableClass) throws IOException {
        initEntryMemTableTest(entryMemTableClass);
        long ledgerId = 1;
        long entryId = 1;
        byte[] data = new byte[10];
        random.nextBytes(data);
        ByteBuffer buf = ByteBuffer.wrap(data);
        memTable.addEntry(ledgerId, entryId, buf, this);
        buf.rewind();
        EntryKeyValue kv = memTable.getEntry(ledgerId, entryId);
        assertEquals(kv.getLedgerId(), ledgerId);
        assertEquals(kv.getEntryId(), entryId);
        assertEquals(kv.getValueAsByteBuffer().nioBuffer(), buf);
        memTable.flush(this);
    }

    @Override
    public void onSizeLimitReached(Checkpoint cp) throws IOException {
        // No-op
    }

    public void process(long ledgerId, long entryId, ByteBuf entry)
            throws IOException {
        // No-op
    }

    /**
     * Test read/write across snapshot.
     * @throws IOException
     */
    @MethodSource("memTableClass")
    @ParameterizedTest
    public void scanAcrossSnapshot(Class entryMemTableClass) throws IOException {
        initEntryMemTableTest(entryMemTableClass);
        byte[] data = new byte[10];
        List<EntryKeyValue> keyValues = new ArrayList<EntryKeyValue>();
        for (long entryId = 1; entryId < 100; entryId++) {
            for (long ledgerId = 1; ledgerId < 3; ledgerId++) {
                random.nextBytes(data);
                memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this);
                keyValues.add(memTable.getEntry(ledgerId, entryId));
                if (random.nextInt(16) == 0) {
                    memTable.snapshot();
                }
            }
        }

        for (EntryKeyValue kv : keyValues) {
            assertEquals(memTable.getEntry(kv.getLedgerId(), kv.getEntryId()), kv);
        }
        memTable.flush(this, Checkpoint.MAX);
    }

    private class KVFLusher implements SkipListFlusher {
        final Set<EntryKeyValue> keyValues;

        void initEntryMemTableTest(final Set<EntryKeyValue> keyValues) {
            this.keyValues = keyValues;
        }

        @Override
        public void process(long ledgerId, long entryId, ByteBuf entry) throws IOException {
            assertTrue(keyValues.add(new EntryKeyValue(ledgerId, entryId, entry.array())),
                    ledgerId + ":" + entryId + " is duplicate in store!");
        }
    }

    private class NoLedgerFLusher implements SkipListFlusher {
        @Override
        public void process(long ledgerId, long entryId, ByteBuf entry) throws IOException {
            throw new NoLedgerException(ledgerId);
        }
    }

    /**
     * Test flush w/ logMark parameter.
     * @throws IOException
     */
    @MethodSource("memTableClass")
    @ParameterizedTest
    public void flushLogMark(Class entryMemTableClass) throws IOException {
        initEntryMemTableTest(entryMemTableClass);
        Set<EntryKeyValue> flushedKVs = Collections.newSetFromMap(new ConcurrentHashMap<EntryKeyValue, Boolean>());
        KVFLusher flusher = new KVFLusher(flushedKVs);

        curCheckpoint.setCheckPoint(2, 2);

        byte[] data = new byte[10];
        long ledgerId = 100;
        for (long entryId = 1; entryId < 100; entryId++) {
            random.nextBytes(data);
            memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this);
        }

        assertNull(memTable.snapshot(new TestCheckPoint(1, 1)));
        assertNotNull(memTable.snapshot(new TestCheckPoint(3, 3)));

        assertTrue(0 < memTable.flush(flusher));
        assertEquals(0, memTable.flush(flusher));

        curCheckpoint.setCheckPoint(4, 4);

        random.nextBytes(data);
        memTable.addEntry(ledgerId, 101, ByteBuffer.wrap(data), this);
        assertEquals(0, memTable.flush(flusher));

        assertEquals(0, memTable.flush(flusher, new TestCheckPoint(3, 3)));
        assertTrue(0 < memTable.flush(flusher, new TestCheckPoint(4, 5)));
    }

    /**
     * Test snapshot/flush interaction.
     * @throws IOException
     */
    @MethodSource("memTableClass")
    @ParameterizedTest
    public void flushSnapshot(Class entryMemTableClass) throws IOException {
        initEntryMemTableTest(entryMemTableClass);
        HashSet<EntryKeyValue> keyValues = new HashSet<EntryKeyValue>();
        Set<EntryKeyValue> flushedKVs = Collections.newSetFromMap(new ConcurrentHashMap<EntryKeyValue, Boolean>());
        KVFLusher flusher = new KVFLusher(flushedKVs);

        byte[] data = new byte[10];
        for (long entryId = 1; entryId < 100; entryId++) {
            for (long ledgerId = 1; ledgerId < 100; ledgerId++) {
                random.nextBytes(data);
                assertTrue(memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0,
                        ledgerId + ":" + entryId + " is duplicate in mem-table!");
                assertTrue(keyValues.add(memTable.getEntry(ledgerId, entryId)),
                        ledgerId + ":" + entryId + " is duplicate in hash-set!");
                if (random.nextInt(16) == 0) {
                    if (null != memTable.snapshot()) {
                        if (random.nextInt(2) == 0) {
                            memTable.flush(flusher);
                        }
                    }
                }
            }
        }

        memTable.flush(flusher, Checkpoint.MAX);
        for (EntryKeyValue kv : keyValues) {
            assertTrue(flushedKVs.contains(kv), "kv " + kv.toString() + " was not flushed!");
        }
    }

    /**
     * Test NoLedger exception/flush interaction.
     * @throws IOException
     */
    @MethodSource("memTableClass")
    @ParameterizedTest
    public void noLedgerException(Class entryMemTableClass) throws IOException {
        initEntryMemTableTest(entryMemTableClass);
        NoLedgerFLusher flusher = new NoLedgerFLusher();

        byte[] data = new byte[10];
        for (long entryId = 1; entryId < 100; entryId++) {
            for (long ledgerId = 1; ledgerId < 100; ledgerId++) {
                random.nextBytes(data);
                if (random.nextInt(16) == 0) {
                    if (null != memTable.snapshot()) {
                        memTable.flush(flusher);
                    }
                }
            }
        }

        memTable.flush(flusher, Checkpoint.MAX);
    }

    private static class TestCheckPoint implements Checkpoint {

        LogMark mark;

        public void initEntryMemTableTest(long fid, long fpos) {
            mark = new LogMark(fid, fpos);
        }

        private void setCheckPoint(long fid, long fpos) {
            mark.setLogMark(fid, fpos);
        }

        @Override
        public int compareTo(Checkpoint o) {
            if (Checkpoint.MAX == o) {
                return -1;
            }
            return mark.compare(((TestCheckPoint) o).mark);
        }

    }

    @MethodSource("memTableClass")
    @ParameterizedTest
    public void getListOfEntriesOfLedger(Class entryMemTableClass) throws IOException {
        initEntryMemTableTest(entryMemTableClass);
        Set<EntryKeyValue> flushedKVs = Collections.newSetFromMap(new ConcurrentHashMap<EntryKeyValue, Boolean>());
        KVFLusher flusher = new KVFLusher(flushedKVs);
        int numofEntries = 100;
        int numOfLedgers = 5;
        byte[] data = new byte[10];
        for (long entryId = 1; entryId <= numofEntries; entryId++) {
            for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
                random.nextBytes(data);
                assertTrue(memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0,
                        ledgerId + ":" + entryId + " is duplicate in mem-table!");
            }
        }
        for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
            OfLong entriesItr = memTable.getListOfEntriesOfLedger((random.nextInt((int) ledgerId) + 1));
            ArrayList<Long> listOfEntries = new ArrayList<Long>();
            Consumer<Long> addMethod = listOfEntries::add;
            entriesItr.forEachRemaining(addMethod);
            assertEquals(numofEntries, listOfEntries.size(), "Number of Entries");
            for (int i = 0; i < numofEntries; i++) {
                assertEquals(Long.valueOf(i + 1), listOfEntries.get(i), "listOfEntries should be sorted");
            }
        }
        assertTrue(memTable.snapshot.isEmpty(), "Snapshot is expected to be empty since snapshot is not done");
        assertTrue(memTable.snapshot() != null, "Take snapshot and returned checkpoint should not be empty");
        assertFalse(memTable.snapshot.isEmpty(), "After taking snapshot, snapshot should not be empty ");
        for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
            OfLong entriesItr = memTable.getListOfEntriesOfLedger((random.nextInt((int) ledgerId) + 1));
            ArrayList<Long> listOfEntries = new ArrayList<Long>();
            Consumer<Long> addMethod = listOfEntries::add;
            entriesItr.forEachRemaining(addMethod);
            assertEquals(numofEntries,
                    listOfEntries.size(),
                    "Number of Entries should be the same even after taking snapshot");
            for (int i = 0; i < numofEntries; i++) {
                assertEquals(Long.valueOf(i + 1), listOfEntries.get(i), "listOfEntries should be sorted");
            }
        }

        memTable.flush(flusher);
        for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
            OfLong entriesItr = memTable.getListOfEntriesOfLedger((random.nextInt((int) ledgerId) + 1));
            assertFalse(entriesItr.hasNext(), "After flushing there shouldn't be entries in memtable");
        }
    }

    @MethodSource("memTableClass")
    @ParameterizedTest
    public void getListOfEntriesOfLedgerFromBothKVMapAndSnapshot(Class entryMemTableClass) throws IOException {
        initEntryMemTableTest(entryMemTableClass);
        int numofEntries = 100;
        int newNumOfEntries = 200;
        int numOfLedgers = 5;
        byte[] data = new byte[10];
        for (long entryId = 1; entryId <= numofEntries; entryId++) {
            for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
                random.nextBytes(data);
                assertTrue(memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0,
                        ledgerId + ":" + entryId + " is duplicate in mem-table!");
            }
        }

        assertTrue(memTable.snapshot.isEmpty(), "Snapshot is expected to be empty since snapshot is not done");
        assertTrue(memTable.snapshot() != null, "Take snapshot and returned checkpoint should not be empty");
        assertFalse(memTable.snapshot.isEmpty(), "After taking snapshot, snapshot should not be empty ");

        for (long entryId = numofEntries + 1; entryId <= newNumOfEntries; entryId++) {
            for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
                random.nextBytes(data);
                assertTrue(memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0,
                        ledgerId + ":" + entryId + " is duplicate in mem-table!");
            }
        }

        for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
            OfLong entriesItr = memTable.getListOfEntriesOfLedger((random.nextInt((int) ledgerId) + 1));
            ArrayList<Long> listOfEntries = new ArrayList<Long>();
            Consumer<Long> addMethod = listOfEntries::add;
            entriesItr.forEachRemaining(addMethod);
            assertEquals(newNumOfEntries, listOfEntries.size(), "Number of Entries should be the same");
            for (int i = 0; i < newNumOfEntries; i++) {
                assertEquals(Long.valueOf(i + 1), listOfEntries.get(i), "listOfEntries should be sorted");
            }
        }
    }

    @MethodSource("memTableClass")
    @ParameterizedTest
    public void getListOfEntriesOfLedgerWhileAddingConcurrently(Class entryMemTableClass) throws IOException, InterruptedException {
        initEntryMemTableTest(entryMemTableClass);
        final int numofEntries = 100;
        final int newNumOfEntries = 200;
        final int concurrentAddOfEntries = 300;
        long ledgerId = 5;
        byte[] data = new byte[10];
        for (long entryId = 1; entryId <= numofEntries; entryId++) {
            random.nextBytes(data);
            assertTrue(memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0,
                    ledgerId + ":" + entryId + " is duplicate in mem-table!");
        }

        assertTrue(memTable.snapshot.isEmpty(), "Snapshot is expected to be empty since snapshot is not done");
        assertTrue(memTable.snapshot() != null, "Take snapshot and returned checkpoint should not be empty");
        assertFalse(memTable.snapshot.isEmpty(), "After taking snapshot, snapshot should not be empty ");

        for (long entryId = numofEntries + 1; entryId <= newNumOfEntries; entryId++) {
            random.nextBytes(data);
            assertTrue(memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0,
                    ledgerId + ":" + entryId + " is duplicate in mem-table!");
        }

        AtomicBoolean successfullyAdded = new AtomicBoolean(true);

        Thread threadToAdd = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (long entryId = newNumOfEntries + 1; entryId <= concurrentAddOfEntries; entryId++) {
                        random.nextBytes(data);
                        boolean thisEntryAddedSuccessfully = (memTable.addEntry(ledgerId, entryId,
                                ByteBuffer.wrap(data), EntryMemTableTest.this) != 0);
                        successfullyAdded.set(successfullyAdded.get() && thisEntryAddedSuccessfully);
                        Thread.sleep(10);
                    }
                } catch (IOException e) {
                    log.error("Got Unexpected exception while adding entries");
                    successfullyAdded.set(false);
                } catch (InterruptedException e) {
                    log.error("Got InterruptedException while waiting");
                    successfullyAdded.set(false);
                }
            }
        });
        threadToAdd.start();

        Thread.sleep(200);
        OfLong entriesItr = memTable.getListOfEntriesOfLedger(ledgerId);
        ArrayList<Long> listOfEntries = new ArrayList<Long>();
        while (entriesItr.hasNext()) {
            listOfEntries.add(entriesItr.next());
            Thread.sleep(5);
        }
        threadToAdd.join(5000);
        assertTrue(successfullyAdded.get(), "Entries should be added successfully in the spawned thread");

        for (int i = 0; i < newNumOfEntries; i++) {
            assertEquals(Long.valueOf(i + 1), listOfEntries.get(i), "listOfEntries should be sorted");
        }
    }

    @MethodSource("memTableClass")
    @ParameterizedTest
    public void addSameEntries(Class entryMemTableClass) throws IOException {
        initEntryMemTableTest(entryMemTableClass);
        final long ledgerId = 1;
        final long entryId = 1;
        final int size = 10;
        final byte[] bytes = new byte[size];
        final int initialPermits = memTable.skipListSemaphore.availablePermits();

        for (int i = 0; i < 5; i++) {
            memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(bytes), this);
            assertEquals(1, memTable.kvmap.size());
            assertEquals(memTable.skipListSemaphore.availablePermits(), initialPermits - size);
        }

        memTable.snapshot(Checkpoint.MAX);
        memTable.flush(this);
        assertEquals(0, memTable.kvmap.size());
        assertEquals(memTable.skipListSemaphore.availablePermits(), initialPermits);
    }
}

