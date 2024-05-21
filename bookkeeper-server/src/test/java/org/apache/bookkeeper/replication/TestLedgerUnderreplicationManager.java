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

package org.apache.bookkeeper.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.protobuf.TextFormat;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.proto.DataFormats.UnderreplicatedLedgerFormat;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the zookeeper implementation of the ledger replication manager.
 */
class TestLedgerUnderreplicationManager {
    static final Logger LOG = LoggerFactory.getLogger(TestLedgerUnderreplicationManager.class);

    ZooKeeperUtil zkUtil = null;

    ServerConfiguration conf = null;
    ExecutorService executor = null;
    LedgerManagerFactory lmf1 = null;
    LedgerManagerFactory lmf2 = null;
    ZooKeeper zkc1 = null;
    ZooKeeper zkc2 = null;

    String basePath;
    String urLedgerPath;
    boolean isLedgerReplicationDisabled = true;

    @BeforeEach
    void setupZooKeeper() throws Exception {
        zkUtil = new ZooKeeperUtil();
        zkUtil.startCluster();

        conf = TestBKConfiguration.newServerConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        executor = Executors.newCachedThreadPool();

        zkc1 = ZooKeeperClient.newBuilder()
                .connectString(zkUtil.getZooKeeperConnectString())
                .sessionTimeoutMs(10000)
                .build();
        zkc2 = ZooKeeperClient.newBuilder()
                .connectString(zkUtil.getZooKeeperConnectString())
                .sessionTimeoutMs(10000)
                .build();

        String zkLedgersRootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf);

        basePath = zkLedgersRootPath + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE;
        urLedgerPath = basePath
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;

        lmf1 = AbstractZkLedgerManagerFactory.newLedgerManagerFactory(
            conf,
            new ZkLayoutManager(
                zkc1,
                zkLedgersRootPath,
                ZkUtils.getACLs(conf)));
        lmf2 = AbstractZkLedgerManagerFactory.newLedgerManagerFactory(
            conf,
            new ZkLayoutManager(
                zkc2,
                zkLedgersRootPath,
                ZkUtils.getACLs(conf)));

    }

    @AfterEach
    void teardownZooKeeper() throws Exception {
        if (zkUtil != null) {
            zkUtil.killCluster();
            zkUtil = null;
        }
        if (executor != null) {
            executor = null;
        }
        if (zkc1 != null) {
            zkc1.close();
            zkc1 = null;
        }
        if (zkc2 != null) {
            zkc2.close();
            zkc2 = null;
        }
        if (lmf1 != null) {
            lmf1.close();
            lmf1 = null;
        }
        if (lmf2 != null) {
            lmf2.close();
            lmf2 = null;
        }
    }

    private Future<Long> getLedgerToReplicate(final LedgerUnderreplicationManager m) {
        return executor.submit(new Callable<Long>() {
                public Long call() {
                    try {
                        return m.getLedgerToRereplicate();
                    } catch (Exception e) {
                        LOG.error("Error getting ledger id", e);
                        return -1L;
                    }
                }
            });
    }

    /**
     * Test basic interactions with the ledger underreplication
     * manager.
     * Mark some ledgers as underreplicated.
     * Ensure that getLedgerToReplicate will block until it a ledger
     * becomes available.
     */
    @Test
    void basicInteraction() throws Exception {
        Set<Long> ledgers = new HashSet<Long>();
        ledgers.add(0xdeadbeefL);
        ledgers.add(0xbeefcafeL);
        ledgers.add(0xffffbeefL);
        ledgers.add(0xfacebeefL);
        String missingReplica = "localhost:3181";

        int count = 0;
        LedgerUnderreplicationManager m = lmf1.newLedgerUnderreplicationManager();
        Iterator<Long> iter = ledgers.iterator();
        while (iter.hasNext()) {
            m.markLedgerUnderreplicated(iter.next(), missingReplica);
            count++;
        }

        List<Future<Long>> futures = new ArrayList<Future<Long>>();
        for (int i = 0; i < count; i++) {
            futures.add(getLedgerToReplicate(m));
        }

        for (Future<Long> f : futures) {
            Long l = f.get(5, TimeUnit.SECONDS);
            assertTrue(ledgers.remove(l));
        }

        Future<Long> f = getLedgerToReplicate(m);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        Long newl = 0xfefefefefefeL;
        m.markLedgerUnderreplicated(newl, missingReplica);
        assertEquals(newl, f.get(5, TimeUnit.SECONDS), "Should have got the one just added");
    }

    /**
     * Test locking for ledger unreplication manager.
     * If there's only one ledger marked for rereplication,
     * and one client has it, it should be locked; another
     * client shouldn't be able to get it. If the first client dies
     * however, the second client should be able to get it.
     */
    @Test
    void locking() throws Exception {
        String missingReplica = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf1.newLedgerUnderreplicationManager();
        LedgerUnderreplicationManager m2 = lmf2.newLedgerUnderreplicationManager();

        Long ledger = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledger, missingReplica);
        Future<Long> f = getLedgerToReplicate(m1);
        Long l = f.get(5, TimeUnit.SECONDS);
        assertEquals(ledger, l, "Should be the ledger I just marked");

        f = getLedgerToReplicate(m2);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        zkc1.close(); // should kill the lock
        zkc1 = null;

        l = f.get(5, TimeUnit.SECONDS);
        assertEquals(ledger, l, "Should be the ledger I marked");
    }


    /**
     * Test that when a ledger has been marked as replicated, it
     * will not be offered to anther client.
     * This test checked that by marking two ledgers, and acquiring
     * them on a single client. It marks one as replicated and then
     * the client is killed. We then check that another client can
     * acquire a ledger, and that it's not the one that was previously
     * marked as replicated.
     */
    @Test
    void markingAsReplicated() throws Exception {
        String missingReplica = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf1.newLedgerUnderreplicationManager();
        LedgerUnderreplicationManager m2 = lmf2.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        Long ledgerB = 0xdefadebL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica);
        m1.markLedgerUnderreplicated(ledgerB, missingReplica);

        Future<Long> fA = getLedgerToReplicate(m1);
        Future<Long> fB = getLedgerToReplicate(m1);

        Long lA = fA.get(5, TimeUnit.SECONDS);
        Long lB = fB.get(5, TimeUnit.SECONDS);

        assertTrue((lA.equals(ledgerA) && lB.equals(ledgerB))
                   || (lA.equals(ledgerB) && lB.equals(ledgerA)),
                   "Should be the ledgers I just marked");

        Future<Long> f = getLedgerToReplicate(m2);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        m1.markLedgerReplicated(lA);
        zkc1.close(); // should kill the lock
        zkc1 = null;

        Long l = f.get(5, TimeUnit.SECONDS);
        assertEquals(lB, l, "Should be the ledger I marked");
    }

    /**
     * Test releasing of a ledger
     * A ledger is released when a client decides it does not want
     * to replicate it (or cannot at the moment).
     * When a client releases a previously acquired ledger, another
     * client should then be able to acquire it.
     */
    @Test
    void release() throws Exception {
        String missingReplica = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf1.newLedgerUnderreplicationManager();
        LedgerUnderreplicationManager m2 = lmf2.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        Long ledgerB = 0xdefadebL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica);
        m1.markLedgerUnderreplicated(ledgerB, missingReplica);

        Future<Long> fA = getLedgerToReplicate(m1);
        Future<Long> fB = getLedgerToReplicate(m1);

        Long lA = fA.get(5, TimeUnit.SECONDS);
        Long lB = fB.get(5, TimeUnit.SECONDS);

        assertTrue((lA.equals(ledgerA) && lB.equals(ledgerB))
                   || (lA.equals(ledgerB) && lB.equals(ledgerA)),
                   "Should be the ledgers I just marked");

        Future<Long> f = getLedgerToReplicate(m2);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        m1.markLedgerReplicated(lA);
        m1.releaseUnderreplicatedLedger(lB);

        Long l = f.get(5, TimeUnit.SECONDS);
        assertEquals(lB, l, "Should be the ledger I marked");
    }

    /**
     * Test that when a failure occurs on a ledger, while the ledger
     * is already being rereplicated, the ledger will still be in the
     * under replicated ledger list when first rereplicating client marks
     * it as replicated.
     */
    @Test
    void manyFailures() throws Exception {
        String missingReplica1 = "localhost:3181";
        String missingReplica2 = "localhost:3182";

        LedgerUnderreplicationManager m1 = lmf1.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica1);

        Future<Long> fA = getLedgerToReplicate(m1);
        Long lA = fA.get(5, TimeUnit.SECONDS);

        m1.markLedgerUnderreplicated(ledgerA, missingReplica2);

        assertEquals(lA, ledgerA, "Should be the ledger I just marked");
        m1.markLedgerReplicated(lA);

        Future<Long> f = getLedgerToReplicate(m1);
        lA = f.get(5, TimeUnit.SECONDS);
        assertEquals(lA, ledgerA, "Should be the ledger I had marked previously");
    }

    /**
     * If replicationworker has acquired lock on it, then
     * getReplicationWorkerIdRereplicatingLedger should return
     * ReplicationWorkerId (BookieId) of the ReplicationWorker that is holding
     * lock. If lock for the underreplicated ledger is not yet acquired or if it
     * is released then it is supposed to return null.
     *
     * @throws Exception
     */
    @Test
    void getReplicationWorkerIdRereplicatingLedger() throws Exception {
        String missingReplica1 = "localhost:3181";
        String missingReplica2 = "localhost:3182";

        LedgerUnderreplicationManager m1 = lmf1.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica1);
        m1.markLedgerUnderreplicated(ledgerA, missingReplica2);

        // lock is not yet acquired so replicationWorkerIdRereplicatingLedger
        // should
        assertNull(m1.getReplicationWorkerIdRereplicatingLedger(ledgerA), "ReplicationWorkerId of the lock");

        Future<Long> fA = getLedgerToReplicate(m1);
        Long lA = fA.get(5, TimeUnit.SECONDS);
        assertEquals(lA, ledgerA, "Should be the ledger that was just marked");

        /*
         * ZkLedgerUnderreplicationManager.getLockData uses
         * DNS.getDefaultHost("default") as the bookieId.
         *
         */
        assertEquals(DNS.getDefaultHost("default"),
                m1.getReplicationWorkerIdRereplicatingLedger(ledgerA),
                "ReplicationWorkerId of the lock");

        m1.markLedgerReplicated(lA);

        assertNull(m1.getReplicationWorkerIdRereplicatingLedger(ledgerA), "ReplicationWorkerId of the lock");
    }

    /**
     * Test that when a ledger is marked as underreplicated with
     * the same missing replica twice, only marking as replicated
     * will be enough to remove it from the list.
     */
    @Test
    void test2reportSame() throws Exception {
        String missingReplica1 = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf1.newLedgerUnderreplicationManager();
        LedgerUnderreplicationManager m2 = lmf2.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica1);
        m2.markLedgerUnderreplicated(ledgerA, missingReplica1);

        // verify duplicate missing replica
        UnderreplicatedLedgerFormat.Builder builderA = UnderreplicatedLedgerFormat
                .newBuilder();
        String znode = getUrLedgerZnode(ledgerA);
        byte[] data = zkc1.getData(znode, false, null);
        TextFormat.merge(new String(data, Charset.forName("UTF-8")), builderA);
        List<String> replicaList = builderA.getReplicaList();
        assertEquals(1,
                replicaList.size(),
                "Published duplicate missing replica : " + replicaList);
        assertTrue(replicaList.contains(missingReplica1),
                "Published duplicate missing replica : " + replicaList);

        Future<Long> fA = getLedgerToReplicate(m1);
        Long lA = fA.get(5, TimeUnit.SECONDS);

        assertEquals(lA, ledgerA, "Should be the ledger I just marked");
        m1.markLedgerReplicated(lA);

        Future<Long> f = getLedgerToReplicate(m2);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
    }

    /**
     * Test that multiple LedgerUnderreplicationManagers should be able to take
     * lock and release for same ledger.
     */
    @Test
    void multipleManagersShouldBeAbleToTakeAndReleaseLock()
            throws Exception {
        String missingReplica1 = "localhost:3181";
        final LedgerUnderreplicationManager m1 = lmf1
                .newLedgerUnderreplicationManager();
        final LedgerUnderreplicationManager m2 = lmf2
                .newLedgerUnderreplicationManager();
        Long ledgerA = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica1);
        final int iterationCount = 100;
        final CountDownLatch latch1 = new CountDownLatch(iterationCount);
        final CountDownLatch latch2 = new CountDownLatch(iterationCount);
        Thread thread1 = new Thread() {
            @Override
            public void run() {
                takeLedgerAndRelease(m1, latch1, iterationCount);
            }
        };

        Thread thread2 = new Thread() {
            @Override
            public void run() {
                takeLedgerAndRelease(m2, latch2, iterationCount);
            }
        };
        thread1.start();
        thread2.start();

        // wait until at least one thread completed
        while (!latch1.await(50, TimeUnit.MILLISECONDS)
                && !latch2.await(50, TimeUnit.MILLISECONDS)) {
            Thread.sleep(50);
        }

        m1.close();
        m2.close();

        // After completing 'lock acquire,release' job, it should notify below
        // wait
        latch1.await();
        latch2.await();
    }

    /**
     * Test verifies failures of bookies which are resembling each other.
     *
     * <p>BK servers named like*********************************************
     * 1.cluster.com, 2.cluster.com, 11.cluster.com, 12.cluster.com
     * *******************************************************************
     *
     * <p>BKserver IP:HOST like*********************************************
     * localhost:3181, localhost:318, localhost:31812
     * *******************************************************************
     */
    @Test
    void markSimilarMissingReplica() throws Exception {
        List<String> missingReplica = new ArrayList<String>();
        missingReplica.add("localhost:3181");
        missingReplica.add("localhost:318");
        missingReplica.add("localhost:31812");
        missingReplica.add("1.cluster.com");
        missingReplica.add("2.cluster.com");
        missingReplica.add("11.cluster.com");
        missingReplica.add("12.cluster.com");
        verifyMarkLedgerUnderreplicated(missingReplica);
    }

    /**
     * Test multiple bookie failures for a ledger and marked as underreplicated
     * one after another.
     */
    @Test
    void manyFailuresInAnEnsemble() throws Exception {
        List<String> missingReplica = new ArrayList<String>();
        missingReplica.add("localhost:3181");
        missingReplica.add("localhost:3182");
        verifyMarkLedgerUnderreplicated(missingReplica);
    }

    /**
     * Test disabling the ledger re-replication. After disabling, it will not be
     * able to getLedgerToRereplicate(). This calls will enter into infinite
     * waiting until enabling rereplication process
     */
    @Test
    void disableLedegerReplication() throws Exception {
        final LedgerUnderreplicationManager replicaMgr = lmf1
                .newLedgerUnderreplicationManager();

        // simulate few urLedgers before disabling
        final Long ledgerA = 0xfeadeefdacL;
        final String missingReplica = "localhost:3181";

        // disabling replication
        replicaMgr.disableLedgerReplication();
        LOG.info("Disabled Ledeger Replication");

        try {
            replicaMgr.markLedgerUnderreplicated(ledgerA, missingReplica);
        } catch (UnavailableException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected exception while marking urLedger", e);
            }
            fail("Unexpected exception while marking urLedger" + e.getMessage());
        }

        Future<Long> fA = getLedgerToReplicate(replicaMgr);
        try {
            fA.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // expected behaviour, as the replication is disabled
            isLedgerReplicationDisabled = false;
        }

        assertFalse(isLedgerReplicationDisabled, "Ledger replication is not disabled!");
    }

    /**
     * Test enabling the ledger re-replication. After enableLedegerReplication,
     * should continue getLedgerToRereplicate() task
     */
    @Test
    void enableLedgerReplication() throws Exception {
        isLedgerReplicationDisabled = true;
        final LedgerUnderreplicationManager replicaMgr = lmf1
                .newLedgerUnderreplicationManager();

        // simulate few urLedgers before disabling
        final Long ledgerA = 0xfeadeefdacL;
        final String missingReplica = "localhost:3181";
        try {
            replicaMgr.markLedgerUnderreplicated(ledgerA, missingReplica);
        } catch (UnavailableException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected exception while marking urLedger", e);
            }
            fail("Unexpected exception while marking urLedger" + e.getMessage());
        }

        // disabling replication
        replicaMgr.disableLedgerReplication();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Disabled Ledeger Replication");
        }

        String znodeA = getUrLedgerZnode(ledgerA);
        final CountDownLatch znodeLatch = new CountDownLatch(2);
        String urledgerA = StringUtils.substringAfterLast(znodeA, "/");
        String urLockLedgerA = basePath + "/locks/" + urledgerA;
        zkc1.exists(urLockLedgerA, new Watcher(){
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.NodeCreated) {
                    znodeLatch.countDown();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Recieved node creation event for the zNodePath:"
                                + event.getPath());
                    }
                }

            }});
        // getLedgerToRereplicate is waiting until enable rereplication
        Thread thread1 = new Thread() {
            @Override
            public void run() {
                try {
                    Long lA = replicaMgr.getLedgerToRereplicate();
                    assertEquals(lA,
                            ledgerA,
                            "Should be the ledger I just marked");
                    isLedgerReplicationDisabled = false;
                    znodeLatch.countDown();
                } catch (UnavailableException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Unexpected exception while marking urLedger", e);
                    }
                    isLedgerReplicationDisabled = false;
                }
            }
        };
        thread1.start();

        try {
            assertFalse(znodeLatch.await(1, TimeUnit.SECONDS), "shouldn't complete");
            assertTrue(isLedgerReplicationDisabled,
                    "Ledger replication is not disabled!");
            assertEquals(2, znodeLatch
                    .getCount(), "Failed to disable ledger replication!");

            replicaMgr.enableLedgerReplication();
            znodeLatch.await(5, TimeUnit.SECONDS);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Enabled Ledeger Replication");
            }
            assertFalse(isLedgerReplicationDisabled, "Ledger replication is not disabled!");
            assertEquals(0, znodeLatch
                    .getCount(), "Failed to disable ledger replication!");
        } finally {
            thread1.interrupt();
        }
    }

    /**
     * Test that the hierarchy gets cleaned up as ledgers
     * are marked as fully replicated.
     */
    @Test
    void hierarchyCleanup() throws Exception {
        final LedgerUnderreplicationManager replicaMgr = lmf1
            .newLedgerUnderreplicationManager();
        // 4 ledgers, 2 in the same hierarchy
        long[] ledgers = { 0x00000000deadbeefL, 0x00000000deadbeeeL,
                           0x00000000beefcafeL, 0x00000000cafed00dL };

        for (long l : ledgers) {
            replicaMgr.markLedgerUnderreplicated(l, "localhost:3181");
        }
        // can't simply test top level as we are limited to ledger
        // ids no larger than an int
        String testPath = urLedgerPath + "/0000/0000";
        List<String> children = zkc1.getChildren(testPath, false);
        assertEquals(3, children.size(), "Wrong number of hierarchies");

        int marked = 0;
        while (marked < 3) {
            long l = replicaMgr.getLedgerToRereplicate();
            if (l != ledgers[0]) {
                replicaMgr.markLedgerReplicated(l);
                marked++;
            } else {
                replicaMgr.releaseUnderreplicatedLedger(l);
            }
        }
        children = zkc1.getChildren(testPath, false);
        assertEquals(1, children.size(), "Wrong number of hierarchies");

        long l = replicaMgr.getLedgerToRereplicate();
        assertEquals(ledgers[0], l, "Got wrong ledger");
        replicaMgr.markLedgerReplicated(l);

        children = zkc1.getChildren(urLedgerPath, false);
        assertEquals(0, children.size(), "All hierarchies should be cleaned up");
    }

    /**
     * Test that as the hierarchy gets cleaned up, it doesn't interfere
     * with the marking of other ledgers as underreplicated.
     */
    @Test
    void hierarchyCleanupInterference() throws Exception {
        final LedgerUnderreplicationManager replicaMgr1 = lmf1
            .newLedgerUnderreplicationManager();
        final LedgerUnderreplicationManager replicaMgr2 = lmf2
            .newLedgerUnderreplicationManager();

        final int iterations = 100;
        final AtomicBoolean threadFailed = new AtomicBoolean(false);
        Thread markUnder = new Thread() {
                public void run() {
                    long l = 1;
                    try {
                        for (int i = 0; i < iterations; i++) {
                            replicaMgr1.markLedgerUnderreplicated(l, "localhost:3181");
                            l += 10000;
                        }
                    } catch (Exception e) {
                        LOG.error("markUnder Thread failed with exception", e);
                        threadFailed.set(true);
                        return;
                    }
                }
            };
        final AtomicInteger processed = new AtomicInteger(0);
        Thread markRepl = new Thread() {
                public void run() {
                    try {
                        for (int i = 0; i < iterations; i++) {
                            long l = replicaMgr2.getLedgerToRereplicate();
                            replicaMgr2.markLedgerReplicated(l);
                            processed.incrementAndGet();
                        }
                    } catch (Exception e) {
                        LOG.error("markRepl Thread failed with exception", e);
                        threadFailed.set(true);
                        return;
                    }
                }
            };
        markRepl.setDaemon(true);
        markUnder.setDaemon(true);

        markRepl.start();
        markUnder.start();
        markUnder.join();
        assertFalse(threadFailed.get(), "Thread failed to complete");

        int lastProcessed = 0;
        while (true) {
            markRepl.join(10000);
            if (!markRepl.isAlive()) {
                break;
            }
            assertFalse(lastProcessed == processed.get(), "markRepl thread not progressing");
        }
        assertFalse(threadFailed.get(), "Thread failed to complete");

        List<String> children = zkc1.getChildren(urLedgerPath, false);
        for (String s : children) {
            LOG.info("s: {}", s);
        }
        assertEquals(0, children.size(), "All hierarchies should be cleaned up");
    }

    @Test
    void checkAllLedgersCTime() throws Exception {
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr1 = lmf1.newLedgerUnderreplicationManager();
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr2 = lmf2.newLedgerUnderreplicationManager();
        assertEquals(-1, underReplicaMgr1.getCheckAllLedgersCTime());
        long curTime = System.currentTimeMillis();
        underReplicaMgr2.setCheckAllLedgersCTime(curTime);
        assertEquals(curTime, underReplicaMgr1.getCheckAllLedgersCTime());
        curTime = System.currentTimeMillis();
        underReplicaMgr2.setCheckAllLedgersCTime(curTime);
        assertEquals(curTime, underReplicaMgr1.getCheckAllLedgersCTime());
    }

    @Test
    void placementPolicyCheckCTime() throws Exception {
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr1 = lmf1.newLedgerUnderreplicationManager();
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr2 = lmf2.newLedgerUnderreplicationManager();
        assertEquals(-1, underReplicaMgr1.getPlacementPolicyCheckCTime());
        long curTime = System.currentTimeMillis();
        underReplicaMgr2.setPlacementPolicyCheckCTime(curTime);
        assertEquals(curTime, underReplicaMgr1.getPlacementPolicyCheckCTime());
        curTime = System.currentTimeMillis();
        underReplicaMgr2.setPlacementPolicyCheckCTime(curTime);
        assertEquals(curTime, underReplicaMgr1.getPlacementPolicyCheckCTime());
    }

    @Test
    void replicasCheckCTime() throws Exception {
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr1 = lmf1.newLedgerUnderreplicationManager();
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr2 = lmf2.newLedgerUnderreplicationManager();
        assertEquals(-1, underReplicaMgr1.getReplicasCheckCTime());
        long curTime = System.currentTimeMillis();
        underReplicaMgr2.setReplicasCheckCTime(curTime);
        assertEquals(curTime, underReplicaMgr1.getReplicasCheckCTime());
        curTime = System.currentTimeMillis();
        underReplicaMgr2.setReplicasCheckCTime(curTime);
        assertEquals(curTime, underReplicaMgr1.getReplicasCheckCTime());
    }

    private void verifyMarkLedgerUnderreplicated(Collection<String> missingReplica)
            throws KeeperException, InterruptedException, ReplicationException {
        Long ledgerA = 0xfeadeefdacL;
        String znodeA = getUrLedgerZnode(ledgerA);
        LedgerUnderreplicationManager replicaMgr = lmf1
                .newLedgerUnderreplicationManager();
        for (String replica : missingReplica) {
            replicaMgr.markLedgerUnderreplicated(ledgerA, replica);
        }

        String urLedgerA = getData(znodeA);
        UnderreplicatedLedgerFormat.Builder builderA = UnderreplicatedLedgerFormat
                .newBuilder();
        for (String replica : missingReplica) {
            builderA.addReplica(replica);
        }
        List<String> replicaList = builderA.getReplicaList();

        for (String replica : missingReplica) {
            assertTrue(replicaList
                    .contains(replica), "UrLedger:" + urLedgerA
                    + " doesn't contain failed bookie :" + replica);
        }
    }

    private String getData(String znode) {
        try {
            byte[] data = zkc1.getData(znode, false, null);
            return new String(data);
        } catch (KeeperException e) {
            LOG.error("Exception while reading data from znode :" + znode);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Exception while reading data from znode :" + znode);
        }
        return "";

    }

    private String getUrLedgerZnode(long ledgerId) {
        return ZkLedgerUnderreplicationManager.getUrLedgerZnode(urLedgerPath, ledgerId);
    }

    private void takeLedgerAndRelease(final LedgerUnderreplicationManager m,
            final CountDownLatch latch, int numberOfIterations) {
        for (int i = 0; i < numberOfIterations; i++) {
            try {
                long ledgerToRereplicate = m.getLedgerToRereplicate();
                m.releaseUnderreplicatedLedger(ledgerToRereplicate);
            } catch (UnavailableException e) {
                LOG.error("UnavailableException when "
                        + "taking or releasing lock", e);
            }
            latch.countDown();
        }
    }
}
