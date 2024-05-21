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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test verifies the auditor bookie scenarios which will be monitoring the
 * bookie failures.
 */
public class AuditorBookieTest extends BookKeeperClusterTestCase {
    // Depending on the taste, select the amount of logging
    // by decommenting one of the two lines below
    // private static final Logger LOG = Logger.getRootLogger();
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorBookieTest.class);
    private String electionPath;
    private HashMap<String, AuditorElector> auditorElectors = new HashMap<String, AuditorElector>();
    private List<ZooKeeper> zkClients = new LinkedList<ZooKeeper>();

    public AuditorBookieTest() {
        super(6);
    }

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        startAuditorElectors();

        electionPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf)
            + "/underreplication/auditorelection";
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        stopAuditorElectors();
        for (ZooKeeper zk : zkClients) {
            zk.close();
        }
        zkClients.clear();
        super.tearDown();
    }

    /**
     * Test should ensure only one should act as Auditor. Starting/shutdown
     * other than auditor bookie shouldn't initiate re-election and multiple
     * auditors.
     */
    @Test
    void ensureOnlySingleAuditor() throws Exception {
        BookieServer auditor = verifyAuditor();

        // shutdown bookie which is not an auditor
        int indexOf = indexOfServer(auditor);
        int bkIndexDownBookie;
        if (indexOf < lastBookieIndex()) {
            bkIndexDownBookie = indexOf + 1;
        } else {
            bkIndexDownBookie = indexOf - 1;
        }
        shutdownBookie(serverByIndex(bkIndexDownBookie));

        startNewBookie();
        startNewBookie();
        // grace period for the auditor re-election if any
        BookieServer newAuditor = waitForNewAuditor(auditor);
        assertSame(
                auditor, newAuditor, "Auditor re-election is not happened for auditor failure!");
    }

    /**
     * Test Auditor crashes should trigger re-election and another bookie should
     * take over the auditor ship.
     */
    @Test
    void successiveAuditorCrashes() throws Exception {
        BookieServer auditor = verifyAuditor();
        shutdownBookie(auditor);

        BookieServer newAuditor1 = waitForNewAuditor(auditor);
        shutdownBookie(newAuditor1);
        BookieServer newAuditor2 = waitForNewAuditor(newAuditor1);
        assertNotSame(
                auditor, newAuditor2, "Auditor re-election is not happened for auditor failure!");
    }

    /**
     * Test restarting the entire bookie cluster. It shouldn't create multiple
     * bookie auditors.
     */
    @Test
    void bookieClusterRestart() throws Exception {
        BookieServer auditor = verifyAuditor();
        for (AuditorElector auditorElector : auditorElectors.values()) {
            assertTrue(auditorElector
                    .isRunning(), "Auditor elector is not running!");
        }
        stopBKCluster();
        stopAuditorElectors();

        startBKCluster(zkUtil.getMetadataServiceUri());
        startAuditorElectors();
        BookieServer newAuditor = waitForNewAuditor(auditor);
        assertNotSame(
                auditor, newAuditor, "Auditor re-election is not happened for auditor failure!");
    }

    /**
     * Test the vote is deleting from the ZooKeeper during shutdown.
     */
    @Test
    void shutdown() throws Exception {
        BookieServer auditor = verifyAuditor();
        shutdownBookie(auditor);

        // waiting for new auditor
        BookieServer newAuditor = waitForNewAuditor(auditor);
        assertNotSame(
                auditor, newAuditor, "Auditor re-election is not happened for auditor failure!");

        List<String> children = zkc.getChildren(electionPath, false);
        for (String child : children) {
            byte[] data = zkc.getData(electionPath + '/' + child, false, null);
            String bookieIP = new String(data);
            String addr = auditor.getBookieId().toString();
            assertFalse(bookieIP
                    .contains(addr), "AuditorElection cleanup fails");
        }
    }

    /**
     * Test restart of the previous Auditor bookie shouldn't initiate
     * re-election and should create new vote after restarting.
     */
    @Test
    void restartAuditorBookieAfterCrashing() throws Exception {
        BookieServer auditor = verifyAuditor();

        String addr = auditor.getBookieId().toString();

        // restarting Bookie with same configurations.
        ServerConfiguration serverConfiguration = shutdownBookie(auditor);

        auditorElectors.remove(addr);
        startBookie(serverConfiguration);
        // starting corresponding auditor elector

        if (LOG.isDebugEnabled()) {
            LOG.debug("Performing Auditor Election:" + addr);
        }
        startAuditorElector(addr);

        // waiting for new auditor to come
        BookieServer newAuditor = waitForNewAuditor(auditor);
        assertNotSame(
                auditor, newAuditor, "Auditor re-election is not happened for auditor failure!");
        assertNotEquals(auditor
                .getBookieId(), newAuditor.getBookieId(), "No relection after old auditor rejoins");
    }

    private void startAuditorElector(String addr) throws Exception {
        AuditorElector auditorElector = new AuditorElector(addr,
                                                           baseConf);
        auditorElectors.put(addr, auditorElector);
        auditorElector.start();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Starting Auditor Elector");
        }
    }

    private void startAuditorElectors() throws Exception {
        for (BookieId addr : bookieAddresses()) {
            startAuditorElector(addr.toString());
        }
    }

    private void stopAuditorElectors() throws Exception {
        for (AuditorElector auditorElector : auditorElectors.values()) {
            auditorElector.shutdown();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Stopping Auditor Elector!");
            }
        }
    }

    private BookieServer verifyAuditor() throws Exception {
        List<BookieServer> auditors = getAuditorBookie();
        assertEquals(1, auditors
                .size(), "Multiple Bookies acting as Auditor!");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Bookie running as Auditor:" + auditors.get(0));
        }
        return auditors.get(0);
    }

    private List<BookieServer> getAuditorBookie() throws Exception {
        List<BookieServer> auditors = new LinkedList<BookieServer>();
        byte[] data = zkc.getData(electionPath, false, null);
        assertNotNull(data, "Auditor election failed");
        for (int i = 0; i < bookieCount(); i++) {
            BookieServer bks = serverByIndex(i);
            if (new String(data).contains(bks.getBookieId() + "")) {
                auditors.add(bks);
            }
        }
        return auditors;
    }

    private ServerConfiguration shutdownBookie(BookieServer bkServer) throws Exception {
        int index = indexOfServer(bkServer);
        String addr = addressByIndex(index).toString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Shutting down bookie:" + addr);
        }

        // shutdown bookie which is an auditor
        ServerConfiguration conf = killBookie(index);

        // stopping corresponding auditor elector
        auditorElectors.get(addr).shutdown();
        return conf;
    }

    private BookieServer waitForNewAuditor(BookieServer auditor)
            throws Exception {
        BookieServer newAuditor = null;
        int retryCount = 8;
        while (retryCount > 0) {
            try {
                List<BookieServer> auditors = getAuditorBookie();
                if (auditors.size() > 0) {
                    newAuditor = auditors.get(0);
                    if (auditor != newAuditor) {
                        break;
                    }
                }
            } catch (Exception ignore) {
            }
            Thread.sleep(500);
            retryCount--;
        }
        assertNotNull(
                newAuditor,
                "New Auditor is not reelected after auditor crashes");
        verifyAuditor();
        return newAuditor;
    }
}
