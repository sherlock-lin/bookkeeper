/*
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.stream.IntStream;
import org.apache.bookkeeper.bookie.EntryLogManagerForEntryLogPerLedger.BufferedLogChannelWithDirInfo;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestOpStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider.TestStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.lang.mutable.MutableInt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test new log creation.
 */
public class CreateNewLogTest {
    private static final Logger LOG = LoggerFactory
            .getLogger(CreateNewLogTest.class);

    private String[] ledgerDirs;
    private int numDirs = 100;

    @BeforeEach
    void setUp() throws Exception{
        ledgerDirs = new String[numDirs];
        for (int i = 0; i < numDirs; i++){
            File temp = File.createTempFile("bookie", "test");
            temp.delete();
            temp.mkdir();
            File currentTemp = new File(temp.getAbsoluteFile() + "/current");
            currentTemp.mkdir();
            ledgerDirs[i] = temp.getPath();
        }
    }

    @AfterEach
    void tearDown() throws Exception{
        for (int i = 0; i < numDirs; i++){
            File f = new File(ledgerDirs[i]);
            deleteRecursive(f);
        }
    }

    private void deleteRecursive(File f) {
        if (f.isDirectory()){
            for (File c : f.listFiles()){
                deleteRecursive(c);
            }
        }

        f.delete();
    }

    /**
     * Checks if new log file id is verified against all directories.
     *
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-465}
     *
     * @throws Exception
     */
    @Test
    void createNewLog() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of
        // ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        // Extracted from createNewLog()
        String logFileName = Long.toHexString(1) + ".log";
        File dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: {}", dir);
        File newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();

        DefaultEntryLogger el = new DefaultEntryLogger(conf, ledgerDirsManager);
        // Calls createNewLog, and with the number of directories we
        // are using, if it picks one at random it will fail.
        EntryLogManagerForSingleEntryLog entryLogManager = (EntryLogManagerForSingleEntryLog) el.getEntryLogManager();
        entryLogManager.createNewLog(0L);
        LOG.info("This is the current log id: {}", entryLogManager.getCurrentLogId());
        assertTrue(entryLogManager.getCurrentLogId() > 1, "Wrong log id");
    }

    @Test
    void createNewLogWithNoWritableLedgerDirs() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setIsForceGCAllowWhenNoSpace(true);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        // Extracted from createNewLog()
        String logFileName = Long.toHexString(1) + ".log";
        File dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: {}", dir);
        File newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();

        // Now let us move all dirs to filled dirs
        List<File> wDirs = ledgerDirsManager.getWritableLedgerDirs();
        for (File tdir: wDirs) {
            ledgerDirsManager.addToFilledDirs(tdir);
        }

        DefaultEntryLogger el = new DefaultEntryLogger(conf, ledgerDirsManager);
        // Calls createNewLog, and with the number of directories we
        // are using, if it picks one at random it will fail.
        EntryLogManagerForSingleEntryLog entryLogManager = (EntryLogManagerForSingleEntryLog) el.getEntryLogManager();
        entryLogManager.createNewLog(0L);
        LOG.info("This is the current log id: {}", entryLogManager.getCurrentLogId());
        assertTrue(entryLogManager.getCurrentLogId() > 1, "Wrong log id");
    }

    void setSameThreadExecutorForEntryLoggerAllocator(EntryLoggerAllocator entryLoggerAllocator) {
        ExecutorService executorService = entryLoggerAllocator.allocatorExecutor;
        executorService.shutdown();
        entryLoggerAllocator.allocatorExecutor = MoreExecutors.newDirectExecutorService();
    }

    /*
     * entryLogPerLedger is enabled and various scenarios of entrylogcreation are tested
     */
    @Test
    void entryLogPerLedgerCreationWithPreAllocation() throws Exception {
        /*
         * I wish I could shorten this testcase or split it into multiple testcases,
         * but I want to cover a scenario and it requires multiple operations in
         * sequence and validations along the way. Please bear with the length of this
         * testcase, I added as many comments as I can to simplify it.
         */

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setIsForceGCAllowWhenNoSpace(true);
        // preAllocation is Enabled
        conf.setEntryLogFilePreAllocationEnabled(true);
        conf.setEntryLogPerLedgerEnabled(true);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLoggerAllocator entryLoggerAllocator = entryLogger.entryLoggerAllocator;
        EntryLogManagerForEntryLogPerLedger entryLogManager = (EntryLogManagerForEntryLogPerLedger) entryLogger
                .getEntryLogManager();
        // set same thread executor for entryLoggerAllocator's allocatorExecutor
        setSameThreadExecutorForEntryLoggerAllocator(entryLoggerAllocator);

        /*
         * no entrylog will be created during initialization
         */
        int expectedPreAllocatedLogID = -1;
        assertEquals(expectedPreAllocatedLogID, entryLoggerAllocator.getPreallocatedLogId(), "PreallocatedlogId after initialization of Entrylogger");

        int numOfLedgers = 6;

        for (long i = 0; i < numOfLedgers; i++) {
            /* since we are starting creation of new ledgers, entrylogid will be ledgerid */
            entryLogManager.createNewLog(i);
        }

        /*
         * preallocation is enabled so though entryLogId starts with 0, preallocatedLogId would be equal to numOfLedgers
         */
        expectedPreAllocatedLogID = numOfLedgers;
        assertEquals(expectedPreAllocatedLogID,
                entryLoggerAllocator.getPreallocatedLogId(),
                "PreallocatedlogId after creation of logs for ledgers");
        assertEquals(numOfLedgers,
                entryLogManager.getCopyOfCurrentLogs().size(),
                "Number of current ");
        assertEquals(0,
                entryLogManager.getRotatedLogChannels().size(),
                "Number of LogChannels to flush");

        // create dummy entrylog file with id - (expectedPreAllocatedLogID + 1)
        String logFileName = Long.toHexString(expectedPreAllocatedLogID + 1) + ".log";
        File dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: " + dir);
        File newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();

        /*
         * since there is already preexisting entrylog file with id -
         * (expectedPreAllocatedLogIDDuringInitialization + 1), when new
         * entrylog is created it should have
         * (expectedPreAllocatedLogIDDuringInitialization + 2) id
         */
        long rotatedLedger = 1L;
        entryLogManager.createNewLog(rotatedLedger);

        expectedPreAllocatedLogID = expectedPreAllocatedLogID + 2;
        assertEquals(expectedPreAllocatedLogID, entryLoggerAllocator.getPreallocatedLogId(), "PreallocatedlogId ");
        assertEquals(numOfLedgers,
                entryLogManager.getCopyOfCurrentLogs().size(),
                "Number of current ");
        List<DefaultEntryLogger.BufferedLogChannel> rotatedLogChannels = entryLogManager.getRotatedLogChannels();
        assertEquals(1, rotatedLogChannels.size(), "Number of LogChannels rotated");
        assertEquals(rotatedLedger, rotatedLogChannels.iterator().next().getLogId(), "Rotated logchannel logid");
        entryLogger.flush();
        /*
         * when flush is called all the rotatedlogchannels are flushed and
         * removed from rotatedlogchannels list. But here since entrylogId - 0,
         * is not yet rotated and flushed yet, getLeastUnflushedLogId will still
         * return 0.
         */
        rotatedLogChannels = entryLogManager.getRotatedLogChannels();
        assertEquals(0, rotatedLogChannels.size(), "Number of LogChannels rotated");
        assertEquals(0, entryLogger.getLeastUnflushedLogId(), "Least UnflushedLoggerId");

        entryLogManager.createNewLog(0L);
        rotatedLogChannels = entryLogManager.getRotatedLogChannels();
        assertEquals(1, rotatedLogChannels.size(), "Number of LogChannels rotated");
        assertEquals(0, entryLogger.getLeastUnflushedLogId(), "Least UnflushedLoggerId");
        entryLogger.flush();
        /*
         * since both entrylogids 0, 1 are rotated and flushed,
         * leastunFlushedLogId should be 2
         */
        assertEquals(2, entryLogger.getLeastUnflushedLogId(), "Least UnflushedLoggerId");
        expectedPreAllocatedLogID = expectedPreAllocatedLogID + 1;

        /*
         * we should be able to get entryLogMetadata from all the active
         * entrylogs and the logs which are moved toflush list. Since no entry
         * is added, all the meta should be empty.
         */
        for (int i = 0; i <= expectedPreAllocatedLogID; i++) {
            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(i);
            assertTrue(meta.isEmpty(), "EntryLogMetadata should be empty");
            assertEquals(0, meta.getTotalSize(), "EntryLog usage should be 0");
        }
    }

    /**
     * In this testcase entryLogPerLedger is Enabled and entrylogs are created
     * while ledgerdirs are getting full.
     */
    @Test
    void entryLogCreationWithFilledDirs() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        // forceGCAllowWhenNoSpace is disabled
        conf.setIsForceGCAllowWhenNoSpace(false);
        // pre-allocation is not enabled
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setEntryLogPerLedgerEnabled(true);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLoggerAllocator entryLoggerAllocator = entryLogger.entryLoggerAllocator;
        EntryLogManagerForEntryLogPerLedger entryLogManager = (EntryLogManagerForEntryLogPerLedger)
                entryLogger.getEntryLogManager();
        // set same thread executor for entryLoggerAllocator's allocatorExecutor
        setSameThreadExecutorForEntryLoggerAllocator(entryLoggerAllocator);

        int expectedPreAllocatedLogIDDuringInitialization = -1;
        assertEquals(expectedPreAllocatedLogIDDuringInitialization, entryLoggerAllocator.getPreallocatedLogId(), "PreallocatedlogId after initialization of Entrylogger");
        assertNull(entryLogger.entryLoggerAllocator.preallocation, "Preallocation Future of this slot should be null");

        long ledgerId = 0L;

        entryLogManager.createNewLog(ledgerId);

        /*
         * pre-allocation is not enabled, so it would not preallocate for next entrylog
         */
        assertEquals(expectedPreAllocatedLogIDDuringInitialization + 1, entryLoggerAllocator.getPreallocatedLogId(), "PreallocatedlogId after initialization of Entrylogger");

        for (int i = 0; i < numDirs - 1; i++) {
            ledgerDirsManager.addToFilledDirs(BookieImpl.getCurrentDirectory(new File(ledgerDirs[i])));
        }

        /*
         * this is the only non-filled ledgerDir so it should be used for creating new entryLog
         */
        File nonFilledLedgerDir = BookieImpl.getCurrentDirectory(new File(ledgerDirs[numDirs - 1]));

        entryLogManager.createNewLog(ledgerId);
        DefaultEntryLogger.BufferedLogChannel newLogChannel = entryLogManager.getCurrentLogForLedger(ledgerId);
        assertEquals(nonFilledLedgerDir.getAbsolutePath(),
                newLogChannel.getLogFile().getParentFile().getAbsolutePath(),
                "Directory of newly created BufferedLogChannel file");

        ledgerDirsManager.addToFilledDirs(BookieImpl.getCurrentDirectory(new File(ledgerDirs[numDirs - 1])));

        // new entrylog creation should succeed, though there is no writable ledgerDir
        entryLogManager.createNewLog(ledgerId);
    }

    /*
     * In this testcase it is validated if the entryLog is created in the
     * ledgerDir with least number of current active entrylogs
     */
    @Test
    void ledgerDirsUniformityDuringCreation() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        // pre-allocation is not enabled
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setEntryLogPerLedgerEnabled(true);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entrylogManager = (EntryLogManagerForEntryLogPerLedger)
                entryLogger.getEntryLogManager();

        for (long i = 0; i < ledgerDirs.length; i++) {
            entrylogManager.createNewLog(i);
        }

        int numberOfLedgersCreated = ledgerDirs.length;

        assertEquals(1,
                highestFrequencyOfEntryLogsPerLedgerDir(entrylogManager.getCopyOfCurrentLogs()),
                "Highest frequency of entrylogs per ledgerdir");

        long newLedgerId = numberOfLedgersCreated;
        entrylogManager.createNewLog(newLedgerId);
        numberOfLedgersCreated++;

        assertEquals(2,
                highestFrequencyOfEntryLogsPerLedgerDir(entrylogManager.getCopyOfCurrentLogs()),
                "Highest frequency of entrylogs per ledgerdir");

        for (long i = numberOfLedgersCreated; i < 2 * ledgerDirs.length; i++) {
            entrylogManager.createNewLog(i);
        }

        assertEquals(2,
                highestFrequencyOfEntryLogsPerLedgerDir(entrylogManager.getCopyOfCurrentLogs()),
                "Highest frequency of entrylogs per ledgerdir");
    }


    int highestFrequencyOfEntryLogsPerLedgerDir(Set<BufferedLogChannelWithDirInfo> copyOfCurrentLogsWithDirInfo) {
        Map<File, MutableInt> frequencyOfEntryLogsInLedgerDirs = new HashMap<File, MutableInt>();
        for (BufferedLogChannelWithDirInfo logChannelWithDirInfo : copyOfCurrentLogsWithDirInfo) {
            File parentDir = logChannelWithDirInfo.getLogChannel().getLogFile().getParentFile();
            if (frequencyOfEntryLogsInLedgerDirs.containsKey(parentDir)) {
                frequencyOfEntryLogsInLedgerDirs.get(parentDir).increment();
            } else {
                frequencyOfEntryLogsInLedgerDirs.put(parentDir, new MutableInt(1));
            }
        }
        @SuppressWarnings("unchecked")
        int highestFreq = ((Entry<File, MutableInt>) (frequencyOfEntryLogsInLedgerDirs.entrySet().stream()
                .max(Map.Entry.comparingByValue()).get())).getValue().intValue();
        return highestFreq;
    }

    @Test
    void concurrentCreateNewLogWithEntryLogFilePreAllocationEnabled() throws Exception {
        testConcurrentCreateNewLog(true);
    }

    @Test
    void concurrentCreateNewLogWithEntryLogFilePreAllocationDisabled() throws Exception {
        testConcurrentCreateNewLog(false);
    }

    public void testConcurrentCreateNewLog(boolean entryLogFilePreAllocationEnabled) throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of
        // ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setEntryLogFilePreAllocationEnabled(entryLogFilePreAllocationEnabled);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DefaultEntryLogger el = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerBase entryLogManager = (EntryLogManagerBase) el.getEntryLogManager();
        // set same thread executor for entryLoggerAllocator's allocatorExecutor
        setSameThreadExecutorForEntryLoggerAllocator(el.getEntryLoggerAllocator());

        assertEquals(-1,
                el.getPreviousAllocatedEntryLogId(),
                "previousAllocatedEntryLogId after initialization");
        assertEquals(0, el.getLeastUnflushedLogId(), "leastUnflushedLogId after initialization");
        int createNewLogNumOfTimes = 10;
        AtomicBoolean receivedException = new AtomicBoolean(false);

        IntStream.range(0, createNewLogNumOfTimes).parallel().forEach((i) -> {
            try {
                (entryLogManager).createNewLog((long) i);
            } catch (IOException e) {
                LOG.error("Received exception while creating newLog", e);
                receivedException.set(true);
            }
        });

        assertFalse(receivedException.get(), "There shouldn't be any exceptions while creating newlog");
        int expectedPreviousAllocatedEntryLogId = createNewLogNumOfTimes - 1;
        if (entryLogFilePreAllocationEnabled) {
            expectedPreviousAllocatedEntryLogId = createNewLogNumOfTimes;
        }

        assertEquals(
                expectedPreviousAllocatedEntryLogId, el.getPreviousAllocatedEntryLogId(), "previousAllocatedEntryLogId after " + createNewLogNumOfTimes
                + " number of times createNewLog is called");
        assertEquals(createNewLogNumOfTimes - 1,
                entryLogManager.getRotatedLogChannels().size(),
                "Number of RotatedLogChannels");
    }

    @Test
    void createNewLogWithGaps() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of
        // ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setEntryLogFilePreAllocationEnabled(false);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DefaultEntryLogger el = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerBase entryLogManagerBase = (EntryLogManagerBase) el.getEntryLogManager();
        entryLogManagerBase.createNewLog(0L);

        assertEquals(0, el.getPreviousAllocatedEntryLogId(), "previousAllocatedEntryLogId after initialization");

        // Extracted from createNewLog()
        String logFileName = Long.toHexString(1) + ".log";
        File dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: {}", dir);
        File newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();

        entryLogManagerBase.createNewLog(0L);
        assertEquals(2,
                el.getPreviousAllocatedEntryLogId(),
                "previousAllocatedEntryLogId since entrylogid 1 is already taken");

        // Extracted from createNewLog()
        logFileName = Long.toHexString(3) + ".log";
        dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: {}", dir);
        newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();

        entryLogManagerBase.createNewLog(0L);
        assertEquals(4,
                el.getPreviousAllocatedEntryLogId(),
                "previousAllocatedEntryLogId since entrylogid 3 is already taken");
    }

    @Test
    void createNewLogAndCompactionLog() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of
        // ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setEntryLogFilePreAllocationEnabled(true);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        DefaultEntryLogger el = new DefaultEntryLogger(conf, ledgerDirsManager);
        // set same thread executor for entryLoggerAllocator's allocatorExecutor
        setSameThreadExecutorForEntryLoggerAllocator(el.getEntryLoggerAllocator());
        AtomicBoolean receivedException = new AtomicBoolean(false);

        IntStream.range(0, 2).parallel().forEach((i) -> {
            try {
                if (i % 2 == 0) {
                    ((EntryLogManagerBase) el.getEntryLogManager()).createNewLog((long) i);
                } else {
                    el.newCompactionLog(i);
                }
            } catch (IOException e) {
                LOG.error("Received exception while creating newLog", e);
                receivedException.set(true);
            }
        });

        assertFalse(receivedException.get(), "There shouldn't be any exceptions while creating newlog");
        assertEquals(
                2,
                el.getPreviousAllocatedEntryLogId(),
                "previousAllocatedEntryLogId after 2 times createNewLog is called");
    }

    @Test
    void lastIdCompatibleBetweenDefaultAndDirectEntryLogger() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of
        // ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setEntryLogFilePreAllocationEnabled(false);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DefaultEntryLogger el = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerBase entryLogManagerBase = (EntryLogManagerBase) el.getEntryLogManager();
        for (int i = 0; i < 10; i++) {
            entryLogManagerBase.createNewLog(i);
        }

        assertEquals(9, el.getPreviousAllocatedEntryLogId());

        //Mock half ledgerDirs lastId is 3.
        for (int i = 0; i < ledgerDirsManager.getAllLedgerDirs().size() / 2; i++) {
            File dir = ledgerDirsManager.getAllLedgerDirs().get(i);
            LOG.info("Picked this directory: {}", dir);
            el.getEntryLoggerAllocator().setLastLogId(dir, 3);
        }

        el = new DefaultEntryLogger(conf, ledgerDirsManager);
        assertEquals(9, el.getPreviousAllocatedEntryLogId());

        //Mock all ledgerDirs lastId is 3.
        for (int i = 0; i < ledgerDirsManager.getAllLedgerDirs().size(); i++) {
            File dir = ledgerDirsManager.getAllLedgerDirs().get(i);
            LOG.info("Picked this directory: {}", dir);
            el.getEntryLoggerAllocator().setLastLogId(dir, 3);
        }

        el = new DefaultEntryLogger(conf, ledgerDirsManager);
        assertEquals(9, el.getPreviousAllocatedEntryLogId());
    }

    /*
     * In this testcase entrylogs for ledgers are tried to create concurrently.
     */
    @Test
    void concurrentEntryLogCreations() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        // pre-allocation is enabled
        conf.setEntryLogFilePreAllocationEnabled(true);
        conf.setEntryLogPerLedgerEnabled(true);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entrylogManager = (EntryLogManagerForEntryLogPerLedger)
                entryLogger.getEntryLogManager();

        int numOfLedgers = 10;
        int numOfThreadsForSameLedger = 10;
        AtomicInteger createdEntryLogs = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch createdLatch = new CountDownLatch(numOfLedgers * numOfThreadsForSameLedger);

        for (long i = 0; i < numOfLedgers; i++) {
            for (int j = 0; j < numOfThreadsForSameLedger; j++) {
                long ledgerId = i;
                new Thread(() -> {
                    try {
                        startLatch.await();
                        entrylogManager.createNewLog(ledgerId);
                        createdEntryLogs.incrementAndGet();
                        Thread.sleep(2000);
                    } catch (InterruptedException | IOException e) {
                        LOG.error("Got exception while trying to createNewLog for Ledger: " + ledgerId, e);
                    } finally {
                        createdLatch.countDown();
                    }
                }).start();
            }
        }

        startLatch.countDown();
        createdLatch.await(20, TimeUnit.SECONDS);
        assertEquals(numOfLedgers * numOfThreadsForSameLedger, createdEntryLogs.get(), "Created EntryLogs");
        assertEquals(numOfLedgers, entrylogManager.getCopyOfCurrentLogs().size(), "Active currentlogs size");
        assertEquals((numOfThreadsForSameLedger - 1) * numOfLedgers,
                entrylogManager.getRotatedLogChannels().size(),
                "Rotated entrylogs size");
        /*
         * EntryLogFilePreAllocation is Enabled so
         * getPreviousAllocatedEntryLogId would be (numOfLedgers *
         * numOfThreadsForSameLedger) instead of (numOfLedgers *
         * numOfThreadsForSameLedger - 1)
         */
        assertEquals(numOfLedgers * numOfThreadsForSameLedger,
                entryLogger.getPreviousAllocatedEntryLogId(),
                "PreviousAllocatedEntryLogId");
    }

    /*
     * In this testcase metrics of EntryLogManagerForEntryLogPerLedger are
     * validated.
     */
    @Test
    void entryLogManagerMetrics() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(BookKeeperServerStats.ENTRYLOGGER_SCOPE);
        int maximumNumberOfActiveEntryLogs = 3;
        int entryLogPerLedgerCounterLimitsMultFactor = 2;

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        // pre-allocation is enabled
        conf.setEntryLogFilePreAllocationEnabled(true);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setMaximumNumberOfActiveEntryLogs(maximumNumberOfActiveEntryLogs);
        conf.setEntryLogPerLedgerCounterLimitsMultFactor(entryLogPerLedgerCounterLimitsMultFactor);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager, null, statsLogger,
                UnpooledByteBufAllocator.DEFAULT);
        EntryLogManagerForEntryLogPerLedger entrylogManager = (EntryLogManagerForEntryLogPerLedger) entryLogger
                .getEntryLogManager();
        // set same thread executor for entryLoggerAllocator's allocatorExecutor
        setSameThreadExecutorForEntryLoggerAllocator(entryLogger.getEntryLoggerAllocator());

        Counter numOfWriteActiveLedgers = statsLogger.getCounter(BookKeeperServerStats.NUM_OF_WRITE_ACTIVE_LEDGERS);
        Counter numOfWriteLedgersRemovedCacheExpiry = statsLogger
                .getCounter(BookKeeperServerStats.NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_EXPIRY);
        Counter numOfWriteLedgersRemovedCacheMaxSize = statsLogger
                .getCounter(BookKeeperServerStats.NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_MAXSIZE);
        Counter numLedgersHavingMultipleEntrylogs = statsLogger
                .getCounter(BookKeeperServerStats.NUM_LEDGERS_HAVING_MULTIPLE_ENTRYLOGS);
        TestOpStatsLogger entryLogsPerLedger = (TestOpStatsLogger) statsLogger
                .getOpStatsLogger(BookKeeperServerStats.ENTRYLOGS_PER_LEDGER);
        // initially all the counters should be 0
        assertEquals(0, numOfWriteActiveLedgers.get().intValue(), "NUM_OF_WRITE_ACTIVE_LEDGERS");
        assertEquals(0,
                numOfWriteLedgersRemovedCacheExpiry.get().intValue(),
                "NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_EXPIRY");
        assertEquals(0,
                numOfWriteLedgersRemovedCacheMaxSize.get().intValue(),
                "NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_MAXSIZE");
        assertEquals(0,
                numLedgersHavingMultipleEntrylogs.get().intValue(),
                "NUM_LEDGERS_HAVING_MULTIPLE_ENTRYLOGS");
        assertEquals(0, entryLogsPerLedger.getSuccessCount(), "ENTRYLOGS_PER_LEDGER SuccessCount");

        // lid-1 : 3 entrylogs, lid-2 : 2 entrylogs, lid-3 : 1 entrylog
        int numOfEntrylogsForLedger1 = 3;
        createNewLogs(entrylogManager, 1L, numOfEntrylogsForLedger1);
        int numOfEntrylogsForLedger2 = 2;
        createNewLogs(entrylogManager, 2L, numOfEntrylogsForLedger2);
        createNewLogs(entrylogManager, 3L, 1);

        assertEquals(3, numOfWriteActiveLedgers.get().intValue(), "NUM_OF_WRITE_ACTIVE_LEDGERS");
        assertEquals(0,
                numOfWriteLedgersRemovedCacheExpiry.get().intValue(),
                "NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_EXPIRY");
        assertEquals(0,
                numOfWriteLedgersRemovedCacheMaxSize.get().intValue(),
                "NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_MAXSIZE");
        assertEquals(2,
                numLedgersHavingMultipleEntrylogs.get().intValue(),
                "NUM_LEDGERS_HAVING_MULTIPLE_ENTRYLOGS");
        assertEquals(0, entryLogsPerLedger.getSuccessCount(), "ENTRYLOGS_PER_LEDGER SuccessCount");

        /*
         * since entrylog for lid-4 is created and entrylogmap cachesize is 3,
         * lid-1 will be removed from entrylogmap cache
         */
        createNewLogs(entrylogManager, 4L, 1);
        assertEquals(maximumNumberOfActiveEntryLogs,
                numOfWriteActiveLedgers.get().intValue(),
                "NUM_OF_WRITE_ACTIVE_LEDGERS");
        assertEquals(1,
                numOfWriteLedgersRemovedCacheMaxSize.get().intValue(),
                "NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_MAXSIZE");
        assertEquals(0, entryLogsPerLedger.getSuccessCount(), "ENTRYLOGS_PER_LEDGER SuccessCount");

        /*
         * entrylog for lid-5, lid-6, lid-7 are created. Since
         * maximumNumberOfActiveEntryLogs = 3 and
         * entryLogPerLedgerCounterLimitsMultFactor = 2, when the entrylog for
         * lid-7 is created, count of lid-1 should be removed from countermap.
         */
        createNewLogs(entrylogManager, 5L, 1);
        createNewLogs(entrylogManager, 6L, 1);
        createNewLogs(entrylogManager, 7L, 1);
        assertEquals(maximumNumberOfActiveEntryLogs,
                numOfWriteActiveLedgers.get().intValue(),
                "NUM_OF_WRITE_ACTIVE_LEDGERS");
        assertEquals(4,
                numOfWriteLedgersRemovedCacheMaxSize.get().intValue(),
                "NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_MAXSIZE");
        assertEquals(1, entryLogsPerLedger.getSuccessCount(), "ENTRYLOGS_PER_LEDGER SuccessCount");
        assertEquals(0, Double.compare(numOfEntrylogsForLedger1, entryLogsPerLedger.getSuccessAverage()), "ENTRYLOGS_PER_LEDGER average value");

        /*
         * entrylog for new lid-8 is created so one more entry from countermap
         * should be removed.
         */
        createNewLogs(entrylogManager, 8L, 4);
        assertEquals(maximumNumberOfActiveEntryLogs,
                numOfWriteActiveLedgers.get().intValue(),
                "NUM_OF_WRITE_ACTIVE_LEDGERS");
        assertEquals(5,
                numOfWriteLedgersRemovedCacheMaxSize.get().intValue(),
                "NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_MAXSIZE");
        assertEquals(3,
                numLedgersHavingMultipleEntrylogs.get().intValue(),
                "NUM_LEDGERS_HAVING_MULTIPLE_ENTRYLOGS");
        assertEquals(2, entryLogsPerLedger.getSuccessCount(), "ENTRYLOGS_PER_LEDGER SuccessCount");
        assertEquals(0, Double.compare((numOfEntrylogsForLedger1 + numOfEntrylogsForLedger2) / 2.0,
                entryLogsPerLedger.getSuccessAverage()), "ENTRYLOGS_PER_LEDGER average value");

        /*
         * lid-3 is still in countermap. So when new entrylogs are created for
         * lid-3, no new entry from counter should be removed. so
         * entryLogsPerLedger.getSuccessCount() should be still old value. Also,
         * since lid-3 is still in countermap, these new 4 entrylogs should be
         * added to previous value 1 and hence the EntryLogsPerLedger for ledger
         * - 3l should be updated to 5.
         */
        createNewLogs(entrylogManager, 3L, 4);
        assertEquals(6,
                numOfWriteLedgersRemovedCacheMaxSize.get().intValue(),
                "NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_MAXSIZE");
        assertEquals(4,
                numLedgersHavingMultipleEntrylogs.get().intValue(),
                "NUM_LEDGERS_HAVING_MULTIPLE_ENTRYLOGS");
        assertEquals(5,
                entrylogManager.entryLogsPerLedgerCounter.getCounterMap().get(3L).intValue(),
                "Numofentrylogs for ledger: 3l");
        assertEquals(2, entryLogsPerLedger.getSuccessCount(), "ENTRYLOGS_PER_LEDGER SuccessCount");
    }

    /*
     * In this testcase metrics of EntryLogManagerForEntryLogPerLedger are
     * validated.
     */
    @Test
    void entryLogManagerMetricsFromExpiryAspect() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(BookKeeperServerStats.ENTRYLOGGER_SCOPE);

        int entrylogMapAccessExpiryTimeInSeconds = 1;
        int entryLogPerLedgerCounterLimitsMultFactor = 2;

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        // pre-allocation is enabled
        conf.setEntryLogFilePreAllocationEnabled(true);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setEntrylogMapAccessExpiryTimeInSeconds(entrylogMapAccessExpiryTimeInSeconds);
        conf.setEntryLogPerLedgerCounterLimitsMultFactor(entryLogPerLedgerCounterLimitsMultFactor);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager, null, statsLogger,
                UnpooledByteBufAllocator.DEFAULT);
        EntryLogManagerForEntryLogPerLedger entrylogManager = (EntryLogManagerForEntryLogPerLedger) entryLogger
                .getEntryLogManager();
        // set same thread executor for entryLoggerAllocator's allocatorExecutor
        setSameThreadExecutorForEntryLoggerAllocator(entryLogger.getEntryLoggerAllocator());

        Counter numOfWriteLedgersRemovedCacheExpiry = statsLogger
                .getCounter(BookKeeperServerStats.NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_EXPIRY);
        TestOpStatsLogger entryLogsPerLedger = (TestOpStatsLogger) statsLogger
                .getOpStatsLogger(BookKeeperServerStats.ENTRYLOGS_PER_LEDGER);

        int numOfEntrylogsForLedger1 = 3;
        createNewLogs(entrylogManager, 1L, numOfEntrylogsForLedger1);
        assertEquals(0, entryLogsPerLedger.getSuccessCount(), "ENTRYLOGS_PER_LEDGER SuccessCount");
        assertEquals(0,
                numOfWriteLedgersRemovedCacheExpiry.get().intValue(),
                "NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_EXPIRY");

        Thread.sleep(entrylogMapAccessExpiryTimeInSeconds * 1000 + 100);
        entrylogManager.doEntryLogMapCleanup();
        entrylogManager.entryLogsPerLedgerCounter.doCounterMapCleanup();
        assertEquals(1,
                numOfWriteLedgersRemovedCacheExpiry.get().intValue(),
                "NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_EXPIRY");
        assertEquals(0, entryLogsPerLedger.getSuccessCount(), "ENTRYLOGS_PER_LEDGER SuccessCount");

        Thread.sleep(entrylogMapAccessExpiryTimeInSeconds * 1000 + 100);
        entrylogManager.doEntryLogMapCleanup();
        entrylogManager.entryLogsPerLedgerCounter.doCounterMapCleanup();
        assertEquals(1,
                numOfWriteLedgersRemovedCacheExpiry.get().intValue(),
                "NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_EXPIRY");
        assertEquals(1, entryLogsPerLedger.getSuccessCount(), "ENTRYLOGS_PER_LEDGER SuccessCount");
        assertEquals(0, Double.compare(numOfEntrylogsForLedger1, entryLogsPerLedger.getSuccessAverage()), "ENTRYLOGS_PER_LEDGER average value");
    }

    private static void createNewLogs(EntryLogManagerForEntryLogPerLedger entrylogManager, long ledgerId,
            int numOfTimes) throws IOException {
        for (int i = 0; i < numOfTimes; i++) {
            entrylogManager.createNewLog(ledgerId);
        }
    }

    @Test
    void lockConsistency() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        conf.setLedgerDirNames(ledgerDirs);
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setMaximumNumberOfActiveEntryLogs(5);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger count = new AtomicInteger(0);

        /*
         * Inject wait operation in 'getWritableLedgerDirsForNewLog' method of
         * ledgerDirsManager. getWritableLedgerDirsForNewLog will be called when
         * entryLogManager.createNewLog is called.
         */
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold())) {
            /*
             * getWritableLedgerDirsForNewLog is called for the first time, it
             * will await on 'latch' latch before calling super
             * getWritableLedgerDirsForNewLog.
             */
            @Override
            public List<File> getWritableLedgerDirsForNewLog() throws NoWritableLedgerDirException {
                if (count.incrementAndGet() == 1) {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        LOG.error("Got InterruptedException while awaiting for latch countdown", e);
                    }
                }
                return super.getWritableLedgerDirsForNewLog();
            }
        };

        DefaultEntryLogger el = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entryLogManager = (EntryLogManagerForEntryLogPerLedger) el
                .getEntryLogManager();

        long firstLedgerId = 100L;
        AtomicBoolean newLogCreated = new AtomicBoolean(false);

        assertFalse(entryLogManager.getCacheAsMap().containsKey(firstLedgerId),
                "EntryLogManager cacheMap should not contain entry for firstLedgerId");
        assertEquals(0, count.get(), "Value of the count should be 0");
        /*
         * In a new thread, create newlog for 'firstLedgerId' and then set
         * 'newLogCreated' to true. Since this is the first createNewLog call,
         * it is going to be blocked untill latch is countdowned to 0.
         */
        new Thread() {
            @Override
            public void run() {
                try {
                    entryLogManager.createNewLog(firstLedgerId);
                    newLogCreated.set(true);
                } catch (IOException e) {
                    LOG.error("Got IOException while creating new log", e);
                }
            }
        }.start();

        /*
         * Wait until entry for 'firstLedgerId' is created in cacheMap. It will
         * be created because in the other thread createNewLog is called.
         */
        while (!entryLogManager.getCacheAsMap().containsKey(firstLedgerId)) {
            Thread.sleep(200);
        }
        Lock firstLedgersLock = entryLogManager.getLock(firstLedgerId);

        /*
         * since 'latch' is not counteddown, newlog should not be created even
         * after waitign for 2 secs.
         */
        Thread.sleep(2000);
        assertFalse(newLogCreated.get(), "New log shouldn't have created");

        /*
         * create MaximumNumberOfActiveEntryLogs of entrylogs and do cache
         * cleanup, so that the earliest entry from cache will be removed.
         */
        for (int i = 1; i <= conf.getMaximumNumberOfActiveEntryLogs(); i++) {
            entryLogManager.createNewLog(firstLedgerId + i);
        }
        entryLogManager.doEntryLogMapCleanup();
        assertFalse(entryLogManager.getCacheAsMap().containsKey(firstLedgerId),
                "Entry for that ledger shouldn't be there");

        /*
         * now countdown the latch, so that the other thread can make progress
         * with createNewLog and since this entry is evicted from cache,
         * entrylog of the newly created entrylog will be added to
         * rotatedentrylogs.
         */
        latch.countDown();
        while (!newLogCreated.get()) {
            Thread.sleep(200);
        }
        while (entryLogManager.getRotatedLogChannels().size() < 1) {
            Thread.sleep(200);
        }

        /*
         * Entry for 'firstLedgerId' is removed from cache, but even in this
         * case when we get lock for the 'firstLedgerId' it should be the same
         * as we got earlier.
         */
        Lock lockForThatLedgerAfterRemoval = entryLogManager.getLock(firstLedgerId);
        assertEquals(firstLedgersLock,
                lockForThatLedgerAfterRemoval,
                "For a given ledger lock should be the same before and after removal");
    }
}
