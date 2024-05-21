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
package org.apache.bookkeeper.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.replication.ReplicationException.BKAuditException;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests verifies bookie vs ledger mapping generating by the BookieLedgerIndexer.
 */
public class BookieLedgerIndexTest extends BookKeeperClusterTestCase {

    // Depending on the taste, select the amount of logging
    // by decommenting one of the two lines below
    // private final static Logger LOG = Logger.getRootLogger();
    private static final Logger LOG = LoggerFactory
            .getLogger(BookieLedgerIndexTest.class);

    private Random rng; // Random Number Generator
    private ArrayList<byte[]> entries; // generated entries
    private final DigestType digestType = DigestType.CRC32;
    private int numberOfLedgers = 3;
    private List<Long> ledgerList;
    private LedgerManagerFactory newLedgerManagerFactory;
    private LedgerManager ledgerManager;

    public BookieLedgerIndexTest()
        throws IOException, KeeperException, InterruptedException {
        this("org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory");
    }

    BookieLedgerIndexTest(String ledgerManagerFactory)
            throws IOException, KeeperException, InterruptedException {
        super(3);
        LOG.info("Running test case using ledger manager : "
                + ledgerManagerFactory);
        // set ledger manager name
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        rng = new Random(System.currentTimeMillis()); // Initialize the Random
        // Number Generator
        entries = new ArrayList<byte[]>(); // initialize the entries list
        ledgerList = new ArrayList<Long>(3);
        // initialize ledger manager
        newLedgerManagerFactory = AbstractZkLedgerManagerFactory.newLedgerManagerFactory(
            baseConf,
            new ZkLayoutManager(zkc,
                ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf), ZkUtils.getACLs(baseConf)));

        ledgerManager = newLedgerManagerFactory.newLedgerManager();
    }

    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
        if (null != newLedgerManagerFactory) {
            newLedgerManagerFactory.close();
            newLedgerManagerFactory = null;
        }
        if (null != ledgerManager) {
            ledgerManager.close();
            ledgerManager = null;
        }
    }

    /**
     * Verify the bookie-ledger mapping with minimum number of bookies and few
     * ledgers.
     */
    @Test
    void simpleBookieLedgerMapping() throws Exception {

        for (int i = 0; i < numberOfLedgers; i++) {
            createAndAddEntriesToLedger().close();
        }

        BookieLedgerIndexer bookieLedgerIndex = new BookieLedgerIndexer(
                ledgerManager);

        Map<String, Set<Long>> bookieToLedgerIndex = bookieLedgerIndex
                .getBookieToLedgerIndex();

        assertEquals(3,
                bookieToLedgerIndex.size(),
                "Missed few bookies in the bookie-ledger mapping!");
        Collection<Set<Long>> bk2ledgerEntry = bookieToLedgerIndex.values();
        for (Set<Long> ledgers : bk2ledgerEntry) {
            assertEquals(3,
                    ledgers.size(),
                    "Missed few ledgers in the bookie-ledger mapping!");
            for (Long ledgerId : ledgers) {
                assertTrue(ledgerList
                        .contains(ledgerId), "Unknown ledger-bookie mapping");
            }
        }
    }

    /**
     * Verify ledger index with failed bookies and throws exception.
     */
    @SuppressWarnings("deprecation")
    @Test
    void withoutZookeeper() throws Exception {
        // This test case is for ledger metadata that stored in ZooKeeper. As
        // far as MSLedgerManagerFactory, ledger metadata are stored in other
        // storage. So this test is not suitable for MSLedgerManagerFactory.
        if (newLedgerManagerFactory instanceof org.apache.bookkeeper.meta.MSLedgerManagerFactory) {
            return;
        }

        for (int i = 0; i < numberOfLedgers; i++) {
            createAndAddEntriesToLedger().close();
        }

        BookieLedgerIndexer bookieLedgerIndex = new BookieLedgerIndexer(
                ledgerManager);
        stopZKCluster();
        try {
            bookieLedgerIndex.getBookieToLedgerIndex();
            fail("Must throw exception as zookeeper are not running!");
        } catch (BKAuditException bkAuditException) {
            // expected behaviour
        }
    }

    /**
     * Verify indexing with multiple ensemble reformation.
     */
    @Test
    void ensembleReformation() throws Exception {
        try {
            LedgerHandle lh1 = createAndAddEntriesToLedger();
            LedgerHandle lh2 = createAndAddEntriesToLedger();

            startNewBookie();
            shutdownBookie(lastBookieIndex() - 1);

            // add few more entries after ensemble reformation
            for (int i = 0; i < 10; i++) {
                ByteBuffer entry = ByteBuffer.allocate(4);
                entry.putInt(rng.nextInt(Integer.MAX_VALUE));
                entry.position(0);

                entries.add(entry.array());
                lh1.addEntry(entry.array());
                lh2.addEntry(entry.array());
            }

            BookieLedgerIndexer bookieLedgerIndex = new BookieLedgerIndexer(
                    ledgerManager);

            Map<String, Set<Long>> bookieToLedgerIndex = bookieLedgerIndex
                    .getBookieToLedgerIndex();
            assertEquals(4,
                    bookieToLedgerIndex.size(),
                    "Missed few bookies in the bookie-ledger mapping!");
            Collection<Set<Long>> bk2ledgerEntry = bookieToLedgerIndex.values();
            for (Set<Long> ledgers : bk2ledgerEntry) {
                assertEquals(
                        2,
                        ledgers.size(),
                        "Missed few ledgers in the bookie-ledger mapping!");
                for (Long ledgerNode : ledgers) {
                    assertTrue(ledgerList
                            .contains(ledgerNode), "Unknown ledger-bookie mapping");
                }
            }
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }

    private void shutdownBookie(int bkShutdownIndex) throws Exception {
        killBookie(bkShutdownIndex);
    }

    private LedgerHandle createAndAddEntriesToLedger() throws BKException,
            InterruptedException {
        int numEntriesToWrite = 20;
        // Create a ledger
        LedgerHandle lh = bkc.createLedger(digestType, "admin".getBytes());
        LOG.info("Ledger ID: " + lh.getId());
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(Integer.MAX_VALUE));
            entry.position(0);

            entries.add(entry.array());
            lh.addEntry(entry.array());
        }
        ledgerList.add(lh.getId());
        return lh;
    }
}
