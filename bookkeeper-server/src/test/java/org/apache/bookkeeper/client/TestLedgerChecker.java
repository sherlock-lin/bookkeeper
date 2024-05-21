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
package org.apache.bookkeeper.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the functionality of LedgerChecker. This Ledger checker should be able
 * to detect the correct underReplicated fragment
 */
public class TestLedgerChecker extends BookKeeperClusterTestCase {
    private static final byte[] TEST_LEDGER_ENTRY_DATA = "TestCheckerData"
            .getBytes();
    private static final byte[] TEST_LEDGER_PASSWORD = "testpasswd".getBytes();
    private static final Logger LOG = LoggerFactory.getLogger(TestLedgerChecker.class);

    public TestLedgerChecker() {
        super(3);
    }

    class CheckerCallback implements GenericCallback<Set<LedgerFragment>> {
        private Set<LedgerFragment> result = null;
        private CountDownLatch latch = new CountDownLatch(1);

        public void operationComplete(int rc, Set<LedgerFragment> result) {
            this.result = result;
            latch.countDown();
        }

        Set<LedgerFragment> waitAndGetResult() throws InterruptedException {
            latch.await();
            return result;
        }
    }

    /**
     * Tests that the LedgerChecker should detect the underReplicated fragments
     * on multiple Bookie crashes.
     */
    @Test
    void checker() throws Exception {

        LedgerHandle lh = bkc.createLedger(BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);
        startNewBookie();

        for (int i = 0; i < 10; i++) {
            lh.addEntry(TEST_LEDGER_ENTRY_DATA);
        }
        BookieId replicaToKill = lh.getLedgerMetadata().getAllEnsembles()
                .get(0L).get(0);
        LOG.info("Killing {}", replicaToKill);
        killBookie(replicaToKill);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(TEST_LEDGER_ENTRY_DATA);
        }

        Set<LedgerFragment> result = getUnderReplicatedFragments(lh);
        assertNotNull(result, "Result shouldn't be null");
        for (LedgerFragment r : result) {
            LOG.info("unreplicated fragment: {}", r);
        }

        assertEquals(1, result.size(), "Should have one missing fragment");
        assertTrue(result.iterator().next().getAddresses().contains(replicaToKill),
            "Fragment should be missing from first replica");

        BookieId replicaToKill2 = lh.getLedgerMetadata()
                .getAllEnsembles().get(0L).get(1);
        LOG.info("Killing {}", replicaToKill2);
        killBookie(replicaToKill2);

        result = getUnderReplicatedFragments(lh);
        assertNotNull(result, "Result shouldn't be null");
        for (LedgerFragment r : result) {
            LOG.info("unreplicated fragment: {}", r);
        }

        AtomicInteger number = new AtomicInteger();
        result.forEach(ledgerFragment -> number.addAndGet(ledgerFragment.getAddresses().size()));
        assertEquals(3, number.get(), "Should have three missing fragments");
    }

    /**
     * Tests that ledger checker should pick the fragment as bad only if any of
     * the fragment entries not meeting the quorum.
     */
    // /////////////////////////////////////////////////////
    // /////////Ensemble = 3, Quorum = 2 ///////////////////
    // /Sample Ledger meta data should look like////////////
    // /0 a b c /////*entry present in a,b. Now kill c//////
    // /1 a b d ////////////////////////////////////////////
    // /Here even though one BK failed at this stage, //////
    // /we don't have any missed entries. Quorum satisfied//
    // /So, there should not be any missing replicas.///////
    // /////////////////////////////////////////////////////
    @Test
    void shouldNotGetTheFragmentIfThereIsNoMissedEntry()
            throws Exception {

        LedgerHandle lh = bkc.createLedger(3, 2, BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);
        lh.addEntry(TEST_LEDGER_ENTRY_DATA);

        // Entry should have added in first 2 Bookies.

        // Kill the 3rd BK from ensemble.
        List<BookieId> firstEnsemble = lh.getLedgerMetadata()
                .getAllEnsembles().get(0L);
        BookieId lastBookieFromEnsemble = firstEnsemble.get(2);
        LOG.info("Killing " + lastBookieFromEnsemble + " from ensemble="
                + firstEnsemble);
        killBookie(lastBookieFromEnsemble);

        startNewBookie();

        LOG.info("Ensembles after first entry :"
                + lh.getLedgerMetadata().getAllEnsembles());

        // Adding one more entry. Here enseble should be reformed.
        lh.addEntry(TEST_LEDGER_ENTRY_DATA);

        LOG.info("Ensembles after second entry :"
                + lh.getLedgerMetadata().getAllEnsembles());

        Set<LedgerFragment> result = getUnderReplicatedFragments(lh);

        assertNotNull(result, "Result shouldn't be null");

        for (LedgerFragment r : result) {
            LOG.info("unreplicated fragment: {}", r);
        }

        assertEquals(1, result.size(), "Empty fragment should be considered missing");
    }

    /**
     * Tests that LedgerChecker should give two fragments when 2 bookies failed
     * in same ensemble when ensemble = 3, quorum = 2.
     */
    @Test
    void shouldGetTwoFrgamentsIfTwoBookiesFailedInSameEnsemble()
            throws Exception {

        LedgerHandle lh = bkc.createLedger(3, 2, BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);
        startNewBookie();
        startNewBookie();
        lh.addEntry(TEST_LEDGER_ENTRY_DATA);

        List<BookieId> firstEnsemble = lh.getLedgerMetadata()
                .getAllEnsembles().get(0L);

        BookieId firstBookieFromEnsemble = firstEnsemble.get(0);
        killBookie(firstEnsemble, firstBookieFromEnsemble);

        BookieId secondBookieFromEnsemble = firstEnsemble.get(1);
        killBookie(firstEnsemble, secondBookieFromEnsemble);
        lh.addEntry(TEST_LEDGER_ENTRY_DATA);
        Set<LedgerFragment> result = getUnderReplicatedFragments(lh);

        assertNotNull(result, "Result shouldn't be null");

        for (LedgerFragment r : result) {
            LOG.info("unreplicated fragment: {}", r);
        }

        assertEquals(2, result.size(), "Empty fragment should be considered missing");
        assertEquals(2, result.iterator().next().getBookiesIndexes().size(), "There should be 2 failed bookies in the fragment");
    }

    /**
     * Tests that LedgerChecker should not get any underReplicated fragments, if
     * corresponding ledger does not exists.
     */
    @Test
    void shouldNotGetAnyFragmentIfNoLedgerPresent()
            throws Exception {

        LedgerHandle lh = bkc.createLedger(3, 2, BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);

        List<BookieId> firstEnsemble = lh.getLedgerMetadata()
                .getAllEnsembles().get(0L);
        BookieId firstBookieFromEnsemble = firstEnsemble.get(0);
        killBookie(firstBookieFromEnsemble);
        startNewBookie();
        lh.addEntry(TEST_LEDGER_ENTRY_DATA);
        bkc.deleteLedger(lh.getId());
        LOG.info("Waiting to see ledger id {} deletion", lh.getId());
        int retries = 40;
        boolean noSuchLedger = false;
        while (retries > 0) {
            try {
                lh.readEntries(0, 0);
            } catch (BKException.BKNoSuchLedgerExistsException bkn) {
                noSuchLedger = true;
                break;
            }
            retries--;
            Thread.sleep(500);
        }
        assertTrue(noSuchLedger, "Ledger exists");
        Set<LedgerFragment> result = getUnderReplicatedFragments(lh);
        assertNotNull(result, "Result shouldn't be null");

        assertEquals(0, result.size(), "There should be 0 fragments. But returned fragments are "
                + result);
    }

    /**
     * Tests that LedgerChecker should get failed ensemble number of fragments
     * if ensemble bookie failures on next entry.
     */
    @Test
    void shouldGetFailedEnsembleNumberOfFgmntsIfEnsembleBookiesFailedOnNextWrite()
            throws Exception {

        startNewBookie();
        startNewBookie();
        LedgerHandle lh = bkc.createLedger(3, 2, BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);
        for (int i = 0; i < 3; i++) {
            lh.addEntry(TEST_LEDGER_ENTRY_DATA);
        }

        // Kill all three bookies
        List<BookieId> firstEnsemble = lh.getLedgerMetadata()
                .getAllEnsembles().get(0L);
        for (BookieId bkAddr : firstEnsemble) {
            killBookie(firstEnsemble, bkAddr);
        }

        Set<LedgerFragment> result = getUnderReplicatedFragments(lh);

        assertNotNull(result, "Result shouldn't be null");

        for (LedgerFragment r : result) {
            LOG.info("unreplicated fragment: {}", r);
        }

        assertEquals(1, result.size(), "There should be 1 fragments");
        assertEquals(3, result.iterator().next().getBookiesIndexes().size(), "There should be 3 failed bookies in the fragment");
    }

    /**
     * Tests that LedgerChecker should not get any fragments as underReplicated
     * if Ledger itself is empty.
     */
    @Test
    void shouldNotGetAnyFragmentWithEmptyLedger() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 2, BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);
        Set<LedgerFragment> result = getUnderReplicatedFragments(lh);
        assertNotNull(result, "Result shouldn't be null");
        assertEquals(0, result.size(), "There should be 0 fragments. But returned fragments are "
                + result);
    }

    /**
     * Tests that LedgerChecker should get all fragments if ledger is empty
     * but all bookies in the ensemble are down.
     * In this case, there's no way to tell whether data was written or not.
     * In this case, there'll only be two fragments, as quorum is 2 and we only
     * suspect that the first entry of the ledger could exist.
     */
    @Test
    void shouldGet2FragmentsWithEmptyLedgerButBookiesDead() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 2, BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);
        for (BookieId b : lh.getLedgerMetadata().getAllEnsembles().get(0L)) {
            killBookie(b);
        }
        Set<LedgerFragment> result = getUnderReplicatedFragments(lh);
        assertNotNull(result, "Result shouldn't be null");
        assertEquals(1, result.size(), "There should be 1 fragments.");
        assertEquals(3, result.iterator().next().getBookiesIndexes().size(), "There should be 3 failed bookies in the fragment");
    }

    /**
     * Tests that LedgerChecker should one fragment as underReplicated
     * if there is an open ledger with single entry written.
     */
    @Test
    void shouldGetOneFragmentWithSingleEntryOpenedLedger() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);
        lh.addEntry(TEST_LEDGER_ENTRY_DATA);
        List<BookieId> firstEnsemble = lh.getLedgerMetadata()
                .getAllEnsembles().get(0L);
        BookieId lastBookieFromEnsemble = firstEnsemble.get(0);
        LOG.info("Killing " + lastBookieFromEnsemble + " from ensemble="
                + firstEnsemble);
        killBookie(lastBookieFromEnsemble);

        startNewBookie();

        //Open ledger separately for Ledger checker.
        LedgerHandle lh1 = bkc.openLedgerNoRecovery(lh.getId(), BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);

        Set<LedgerFragment> result = getUnderReplicatedFragments(lh1);
        assertNotNull(result, "Result shouldn't be null");
        assertEquals(1, result.size(), "There should be 1 fragment. But returned fragments are "
                + result);
        assertEquals(1, result.iterator().next().getBookiesIndexes().size(), "There should be 1 failed bookies in the fragment");
    }

    /**
     * Tests that LedgerChecker correctly identifies missing fragments
     * when a single entry is written after an ensemble change.
     * This is important, as the last add confirmed may be less than the
     * first entry id of the final segment.
     */
    @Test
    void singleEntryAfterEnsembleChange() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);
        for (int i = 0; i < 10; i++) {
            lh.addEntry(TEST_LEDGER_ENTRY_DATA);
        }
        List<BookieId> firstEnsemble = lh.getLedgerMetadata()
                .getAllEnsembles().get(0L);
        DistributionSchedule.WriteSet writeSet = lh.getDistributionSchedule().getWriteSet(lh.getLastAddPushed());
        BookieId lastBookieFromEnsemble = firstEnsemble.get(writeSet.get(0));
        LOG.info("Killing " + lastBookieFromEnsemble + " from ensemble="
                + firstEnsemble);
        killBookie(lastBookieFromEnsemble);
        startNewBookie();

        lh.addEntry(TEST_LEDGER_ENTRY_DATA);

        writeSet = lh.getDistributionSchedule().getWriteSet(
                lh.getLastAddPushed());
        lastBookieFromEnsemble = firstEnsemble.get(writeSet.get(1));
        LOG.info("Killing " + lastBookieFromEnsemble + " from ensemble="
                + firstEnsemble);
        killBookie(lastBookieFromEnsemble);

        //Open ledger separately for Ledger checker.
        LedgerHandle lh1 = bkc.openLedgerNoRecovery(lh.getId(), BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);

        Set<LedgerFragment> result = getUnderReplicatedFragments(lh1);
        assertNotNull(result, "Result shouldn't be null");
        assertEquals(2, result.size(), "There should be 2 fragments. But returned fragments are "
                + result);
        for (LedgerFragment lf : result) {
            if (lf.getFirstEntryId() == 0L) {
                assertEquals(2, lf.getBookiesIndexes().size(), "There should be 2 failed bookies in first fragment");
            } else {
                assertEquals(1, lf.getBookiesIndexes().size(), "There should be 1 failed bookie in second fragment");
            }
        }
    }

    /**
     * Tests that LedgerChecker does not return any fragments
     * from a closed ledger with 0 entries.
     */
    @Test
    void closedEmptyLedger() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);
        List<BookieId> firstEnsemble = lh.getLedgerMetadata()
                .getAllEnsembles().get(0L);
        lh.close();

        BookieId lastBookieFromEnsemble = firstEnsemble.get(0);
        LOG.info("Killing " + lastBookieFromEnsemble + " from ensemble="
                + firstEnsemble);
        killBookie(lastBookieFromEnsemble);

        //Open ledger separately for Ledger checker.
        LedgerHandle lh1 = bkc.openLedgerNoRecovery(lh.getId(), BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);

        Set<LedgerFragment> result = getUnderReplicatedFragments(lh1);
        assertNotNull(result, "Result shouldn't be null");
        assertEquals(1, result.size(), "Empty fragment should be considered missing"
                + result);
    }

    /**
     * Tests that LedgerChecker does not return any fragments
     * from a closed ledger with 0 entries.
     */
    @Test
    void closedSingleEntryLedger() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 2, BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);
        List<BookieId> firstEnsemble = lh.getLedgerMetadata()
            .getAllEnsembles().get(0L);
        lh.addEntry(TEST_LEDGER_ENTRY_DATA);
        lh.close();

        // kill bookie 2
        BookieId lastBookieFromEnsemble = firstEnsemble.get(2);
        LOG.info("Killing " + lastBookieFromEnsemble + " from ensemble="
                + firstEnsemble);
        killBookie(lastBookieFromEnsemble);

        //Open ledger separately for Ledger checker.
        LedgerHandle lh1 = bkc.openLedgerNoRecovery(lh.getId(), BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);

        Set<LedgerFragment> result = getUnderReplicatedFragments(lh1);
        assertNotNull(result, "Result shouldn't be null");
        assertEquals(1, result.size(), "Empty fragment should be considered missing"
                + result);
        lh1.close();

        // kill bookie 1
        lastBookieFromEnsemble = firstEnsemble.get(1);
        LOG.info("Killing " + lastBookieFromEnsemble + " from ensemble="
                + firstEnsemble);
        killBookie(lastBookieFromEnsemble);
        startNewBookie();

        //Open ledger separately for Ledger checker.
        lh1 = bkc.openLedgerNoRecovery(lh.getId(), BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);

        result = getUnderReplicatedFragments(lh1);
        assertNotNull(result, "Result shouldn't be null");
        assertEquals(1, result.size(), "There should be 1 fragment. But returned fragments are "
                + result);
        assertEquals(2, result.iterator().next().getBookiesIndexes().size(), "There should be 2 failed bookies in the fragment");
        lh1.close();

        // kill bookie 0
        lastBookieFromEnsemble = firstEnsemble.get(0);
        LOG.info("Killing " + lastBookieFromEnsemble + " from ensemble="
                + firstEnsemble);
        killBookie(lastBookieFromEnsemble);
        startNewBookie();

        //Open ledger separately for Ledger checker.
        lh1 = bkc.openLedgerNoRecovery(lh.getId(), BookKeeper.DigestType.CRC32,
                TEST_LEDGER_PASSWORD);

        result = getUnderReplicatedFragments(lh1);
        assertNotNull(result, "Result shouldn't be null");
        assertEquals(1, result.size(), "There should be 1 fragment. But returned fragments are "
                + result);
        assertEquals(3, result.iterator().next().getBookiesIndexes().size(), "There should be 3 failed bookies in the fragment");
        lh1.close();
    }

    @Test
    void verifyLedgerFragmentSkipsUnavailableBookie() throws Exception {
        // Initialize LedgerChecker with mocked watcher to validate interactions
        BookieWatcher bookieWatcher = mock(BookieWatcher.class);
        when(bookieWatcher.isBookieUnavailable(any())).thenReturn(true);
        LedgerChecker mockedChecker = new LedgerChecker(bkc.getBookieClient(), bookieWatcher);

        LedgerHandle ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, TEST_LEDGER_PASSWORD);

        // Add entries to ensure the right code path is validated
        ledgerHandle.addEntry(TEST_LEDGER_ENTRY_DATA);
        ledgerHandle.addEntry(TEST_LEDGER_ENTRY_DATA);
        ledgerHandle.addEntry(TEST_LEDGER_ENTRY_DATA);

        CheckerCallback cb = new CheckerCallback();
        mockedChecker.checkLedger(ledgerHandle, cb);
        Set<LedgerFragment> result = cb.waitAndGetResult();

        // Note that the bookieWatcher mock is set to make the ledger underreplicated
        assertEquals(1, result.size(), "The one ledger should be considered underreplicated.");
        verify(bookieWatcher, times(3)).isBookieUnavailable(any());
    }

    private Set<LedgerFragment> getUnderReplicatedFragments(LedgerHandle lh)
            throws InterruptedException {
        LedgerChecker checker = new LedgerChecker(bkc, 1);
        CheckerCallback cb = new CheckerCallback();
        checker.checkLedger(lh, cb);
        Set<LedgerFragment> result = cb.waitAndGetResult();
        return result;
    }

    private void killBookie(List<BookieId> firstEnsemble, BookieId ensemble)
            throws Exception {
        LOG.info("Killing " + ensemble + " from ensemble=" + firstEnsemble);
        killBookie(ensemble);
    }

}
