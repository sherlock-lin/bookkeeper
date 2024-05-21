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
package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.STORAGE_SCRUB_PAGE_RETRIES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for InterleavedLedgerStorage.
 */
public class InterleavedLedgerStorageTest {
    private static final Logger LOG = LoggerFactory.getLogger(InterleavedLedgerStorageTest.class);

    public static Iterable<Boolean> elplSetting() {
        return Arrays.asList(true, false);
    }

    public void initInterleavedLedgerStorageTest(boolean elplSetting) {
        conf.setEntryLogSizeLimit(2048);
        conf.setEntryLogPerLedgerEnabled(elplSetting);
    }

    CheckpointSource checkpointSource = new CheckpointSource() {
        @Override
        public Checkpoint newCheckpoint() {
            return Checkpoint.MAX;
        }

        @Override
        public void checkpointComplete(Checkpoint checkpoint, boolean compact) throws IOException {
        }
    };

    Checkpointer checkpointer = new Checkpointer() {
        @Override
        public void startCheckpoint(Checkpoint checkpoint) {
            // No-op
        }

        @Override
        public void start() {
            // no-op
        }
    };

    static class TestableDefaultEntryLogger extends DefaultEntryLogger {
        public interface CheckEntryListener {
            void accept(long ledgerId,
                        long entryId,
                        long entryLogId,
                        long pos);
        }
        volatile CheckEntryListener testPoint;

        public void initInterleavedLedgerStorageTest(
                ServerConfiguration conf,
                LedgerDirsManager ledgerDirsManager,
                EntryLogListener listener,
                StatsLogger statsLogger) throws IOException {
            super(conf, ledgerDirsManager, listener, statsLogger, UnpooledByteBufAllocator.DEFAULT);
        }

        void setCheckEntryTestPoint(CheckEntryListener testPoint) throws InterruptedException {
            this.testPoint = testPoint;
        }

        @Override
        void checkEntry(long ledgerId, long entryId, long location) throws EntryLookupException, IOException {
            CheckEntryListener runBefore = testPoint;
            if (runBefore != null) {
                runBefore.accept(ledgerId, entryId, logIdForOffset(location), posForOffset(location));
            }
            super.checkEntry(ledgerId, entryId, location);
        }
    }

    TestStatsProvider statsProvider = new TestStatsProvider();
    ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
    LedgerDirsManager ledgerDirsManager;
    TestableDefaultEntryLogger entryLogger;
    InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();
    final long numWrites = 2000;
    final long moreNumOfWrites = 3000;
    final long entriesPerWrite = 2;
    final long numOfLedgers = 5;

    @BeforeEach
    void setUp() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        conf.setLedgerDirNames(new String[]{tmpDir.toString()});
        ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        entryLogger = new TestableDefaultEntryLogger(
                conf, ledgerDirsManager, null, NullStatsLogger.INSTANCE);
        interleavedStorage.initializeWithEntryLogger(
                conf, null, ledgerDirsManager, ledgerDirsManager,
                entryLogger, statsProvider.getStatsLogger(BOOKIE_SCOPE));
        interleavedStorage.setCheckpointer(checkpointer);
        interleavedStorage.setCheckpointSource(checkpointSource);

        // Insert some ledger & entries in the interleaved storage
        for (long entryId = 0; entryId < numWrites; entryId++) {
            for (long ledgerId = 0; ledgerId < numOfLedgers; ledgerId++) {
                if (entryId == 0) {
                    interleavedStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
                    interleavedStorage.setFenced(ledgerId);
                }
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId * entriesPerWrite);
                entry.writeBytes(("entry-" + entryId).getBytes());

                interleavedStorage.addEntry(entry);
            }
        }
    }

    @MethodSource("elplSetting")
    @ParameterizedTest
    public void indexEntryIterator(boolean elplSetting) throws Exception {
        initInterleavedLedgerStorageTest(elplSetting);
        try (LedgerCache.PageEntriesIterable pages = interleavedStorage.getIndexEntries(0)) {
            MutableLong curEntry = new MutableLong(0);
            for (LedgerCache.PageEntries page : pages) {
                try (LedgerEntryPage lep = page.getLEP()) {
                    lep.getEntries((entry, offset) -> {
                        assertEquals(curEntry.longValue(), entry);
                        assertNotEquals(0, offset);
                        curEntry.setValue(entriesPerWrite + entry);
                        return true;
                    });
                }
            }
            assertEquals(entriesPerWrite * numWrites, curEntry.longValue());
        }
    }

    @MethodSource("elplSetting")
    @ParameterizedTest
    public void getListOfEntriesOfLedger(boolean elplSetting) throws IOException {
        initInterleavedLedgerStorageTest(elplSetting);
        for (long ledgerId = 0; ledgerId < numOfLedgers; ledgerId++) {
            OfLong entriesOfLedger = interleavedStorage.getListOfEntriesOfLedger(ledgerId);
            ArrayList<Long> arrayList = new ArrayList<Long>();
            Consumer<Long> addMethod = arrayList::add;
            entriesOfLedger.forEachRemaining(addMethod);
            assertEquals(numWrites, arrayList.size(), "Number of entries");
            assertTrue(IntStream.range(0, arrayList.size()).allMatch(i -> {
                return arrayList.get(i) == (i * entriesPerWrite);
            }), "Entries of Ledger");
        }

        long nonExistingLedger = 456789L;
        OfLong entriesOfLedger = interleavedStorage.getListOfEntriesOfLedger(nonExistingLedger);
        assertFalse(entriesOfLedger.hasNext(), "There shouldn't be any entry");
    }

    @MethodSource("elplSetting")
    @ParameterizedTest
    public void getListOfEntriesOfLedgerAfterFlush(boolean elplSetting) throws IOException {
        initInterleavedLedgerStorageTest(elplSetting);
        interleavedStorage.flush();

        // Insert some more ledger & entries in the interleaved storage
        for (long entryId = numWrites; entryId < moreNumOfWrites; entryId++) {
            for (long ledgerId = 0; ledgerId < numOfLedgers; ledgerId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId * entriesPerWrite);
                entry.writeBytes(("entry-" + entryId).getBytes());

                interleavedStorage.addEntry(entry);
            }
        }

        for (long ledgerId = 0; ledgerId < numOfLedgers; ledgerId++) {
            OfLong entriesOfLedger = interleavedStorage.getListOfEntriesOfLedger(ledgerId);
            ArrayList<Long> arrayList = new ArrayList<Long>();
            Consumer<Long> addMethod = arrayList::add;
            entriesOfLedger.forEachRemaining(addMethod);
            assertEquals(moreNumOfWrites, arrayList.size(), "Number of entries");
            assertTrue(IntStream.range(0, arrayList.size()).allMatch(i -> {
                return arrayList.get(i) == (i * entriesPerWrite);
            }), "Entries of Ledger");
        }
    }

    @MethodSource("elplSetting")
    @ParameterizedTest
    public void consistencyCheckConcurrentGC(boolean elplSetting) throws Exception {
        initInterleavedLedgerStorageTest(elplSetting);
        final long signalDone = -1;
        final List<Exception> asyncErrors = new ArrayList<>();
        final LinkedBlockingQueue<Long> toCompact = new LinkedBlockingQueue<>();
        final Semaphore awaitingCompaction = new Semaphore(0);

        interleavedStorage.flush();
        final long lastLogId = entryLogger.getLeastUnflushedLogId();

        final MutableInt counter = new MutableInt(0);
        entryLogger.setCheckEntryTestPoint((ledgerId, entryId, entryLogId, pos) -> {
            if (entryLogId < lastLogId) {
                if (counter.intValue() % 100 == 0) {
                    try {
                        toCompact.put(entryLogId);
                        awaitingCompaction.acquire();
                    } catch (InterruptedException e) {
                        asyncErrors.add(e);
                    }
                }
                counter.increment();
            }
        });

        Thread mutator = new Thread(() -> {
            EntryLogCompactor compactor = new EntryLogCompactor(
                    conf,
                    entryLogger,
                    interleavedStorage,
                    entryLogger::removeEntryLog);
            while (true) {
                Long next = null;
                try {
                    next = toCompact.take();
                    if (next == null || next == signalDone) {
                        break;
                    }
                    compactor.compact(entryLogger.getEntryLogMetadata(next));
                } catch (BufferedChannelBase.BufferedChannelClosedException e) {
                    // next was already removed, ignore
                } catch (Exception e) {
                    asyncErrors.add(e);
                    break;
                } finally {
                    if (next != null) {
                        awaitingCompaction.release();
                    }
                }
            }
        });
        mutator.start();

        List<LedgerStorage.DetectedInconsistency> inconsistencies = interleavedStorage.localConsistencyCheck(
                Optional.empty());
        for (LedgerStorage.DetectedInconsistency e: inconsistencies) {
            LOG.error("Found: {}", e);
        }
        assertEquals(0, inconsistencies.size());

        toCompact.offer(signalDone);
        mutator.join();
        for (Exception e: asyncErrors) {
            throw e;
        }

        if (!conf.isEntryLogPerLedgerEnabled()) {
            assertNotEquals(
                    0,
                    statsProvider.getCounter(BOOKIE_SCOPE + "." + STORAGE_SCRUB_PAGE_RETRIES).get().longValue());
        }
    }

    @MethodSource("elplSetting")
    @ParameterizedTest
    public void consistencyMissingEntry(boolean elplSetting) throws Exception {
        initInterleavedLedgerStorageTest(elplSetting);
        // set 1, 1 to nonsense
        interleavedStorage.ledgerCache.putEntryOffset(1, 1, 0xFFFFFFFFFFFFFFFFL);

        List<LedgerStorage.DetectedInconsistency> errors = interleavedStorage.localConsistencyCheck(Optional.empty());
        assertEquals(1, errors.size());
        LedgerStorage.DetectedInconsistency inconsistency = errors.remove(0);
        assertEquals(1, inconsistency.getEntryId());
        assertEquals(1, inconsistency.getLedgerId());
    }

    @MethodSource("elplSetting")
    @ParameterizedTest
    public void wrongEntry(boolean elplSetting) throws Exception {
        initInterleavedLedgerStorageTest(elplSetting);
        // set 1, 1 to nonsense
        interleavedStorage.ledgerCache.putEntryOffset(
                1,
                1,
                interleavedStorage.ledgerCache.getEntryOffset(0, 0));

        List<LedgerStorage.DetectedInconsistency> errors = interleavedStorage.localConsistencyCheck(Optional.empty());
        assertEquals(1, errors.size());
        LedgerStorage.DetectedInconsistency inconsistency = errors.remove(0);
        assertEquals(1, inconsistency.getEntryId());
        assertEquals(1, inconsistency.getLedgerId());
    }

    @MethodSource("elplSetting")
    @ParameterizedTest
    public void shellCommands(boolean elplSetting) throws Exception {
        initInterleavedLedgerStorageTest(elplSetting);
        interleavedStorage.flush();
        interleavedStorage.shutdown();
        final Pattern entryPattern = Pattern.compile(
                "entry (?<entry>\\d+)\t:\t((?<na>N/A)|\\(log:(?<logid>\\d+), pos: (?<pos>\\d+)\\))");

        class Metadata {
            final Pattern keyPattern = Pattern.compile("master key +: ([0-9a-f])");
            final Pattern sizePattern = Pattern.compile("size +: (\\d+)");
            final Pattern entriesPattern = Pattern.compile("entries +: (\\d+)");
            final Pattern isFencedPattern = Pattern.compile("isFenced +: (\\w+)");

            public String masterKey;
            public long size = -1;
            public long entries = -1;
            public boolean foundFenced = false;

            void check(String s) {
                Matcher keyMatcher = keyPattern.matcher(s);
                if (keyMatcher.matches()) {
                    masterKey = keyMatcher.group(1);
                    return;
                }

                Matcher sizeMatcher = sizePattern.matcher(s);
                if (sizeMatcher.matches()) {
                    size = Long.parseLong(sizeMatcher.group(1));
                    return;
                }

                Matcher entriesMatcher = entriesPattern.matcher(s);
                if (entriesMatcher.matches()) {
                    entries = Long.parseLong(entriesMatcher.group(1));
                    return;
                }

                Matcher isFencedMatcher = isFencedPattern.matcher(s);
                if (isFencedMatcher.matches()) {
                    assertEquals("true", isFencedMatcher.group(1));
                    foundFenced = true;
                    return;
                }
            }

            void validate(long foundEntries) {
                assertTrue(entries >= numWrites * entriesPerWrite);
                assertEquals(entries, foundEntries);
                assertTrue(foundFenced);
                assertNotEquals(-1, size);
            }
        }
        final Metadata foundMetadata = new Metadata();

        AtomicLong curEntry = new AtomicLong(0);
        AtomicLong someEntryLogger = new AtomicLong(-1);
        BookieShell shell = new BookieShell(
                LedgerIdFormatter.LONG_LEDGERID_FORMATTER, EntryFormatter.STRING_FORMATTER) {
            @Override
            void printInfoLine(String s) {
                Matcher matcher = entryPattern.matcher(s);
                System.out.println(s);
                if (matcher.matches()) {
                    assertEquals(Long.toString(curEntry.get()), matcher.group("entry"));

                    if (matcher.group("na") == null) {
                        String logId = matcher.group("logid");
                        assertNotEquals(null, matcher.group("logid"));
                        assertNotEquals(null, matcher.group("pos"));
                        assertEquals(0, (curEntry.get() % entriesPerWrite));
                        assertTrue(curEntry.get() <= numWrites * entriesPerWrite);
                        if (someEntryLogger.get() == -1) {
                            someEntryLogger.set(Long.parseLong(logId));
                        }
                    } else {
                        assertNull(matcher.group("logid"));
                        assertNull(matcher.group("pos"));
                        assertTrue(((curEntry.get() % entriesPerWrite) != 0)
                                || ((curEntry.get() >= (entriesPerWrite * numWrites))));
                    }
                    curEntry.incrementAndGet();
                } else {
                    foundMetadata.check(s);
                }
            }
        };
        shell.setConf(conf);
        int res = shell.run(new String[] { "ledger", "-m", "0" });
        assertEquals(0, res);
        assertTrue(curEntry.get() >= numWrites * entriesPerWrite);
        foundMetadata.validate(curEntry.get());

        // Should pass consistency checker
        res = shell.run(new String[] { "localconsistencycheck" });
        assertEquals(0, res);


        // Remove a logger
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf);
        entryLogger.removeEntryLog(someEntryLogger.get());

        // Should fail consistency checker
        res = shell.run(new String[] { "localconsistencycheck" });
        assertEquals(1, res);
    }
}
