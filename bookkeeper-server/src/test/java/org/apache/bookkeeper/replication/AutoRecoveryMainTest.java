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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.TestUtils;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * Test the AuditorPeer.
 */
public class AutoRecoveryMainTest extends BookKeeperClusterTestCase {

    public AutoRecoveryMainTest() {
        super(3);
    }

    /**
     * Test the startup of the auditorElector and RW.
     */
    @Test
    void startup() throws Exception {
        AutoRecoveryMain main = new AutoRecoveryMain(confByIndex(0));
        try {
            main.start();
            Thread.sleep(500);
            assertTrue(main.auditorElector.isRunning(),
                    "AuditorElector should be running");
            assertTrue(main.replicationWorker.isRunning(),
                    "Replication worker should be running");
        } finally {
            main.shutdown();
        }
    }

    /*
     * Test the shutdown of all daemons
     */
    @Test
    void shutdown() throws Exception {
        AutoRecoveryMain main = new AutoRecoveryMain(confByIndex(0));
        main.start();
        Thread.sleep(500);
        assertTrue(main.auditorElector.isRunning(),
                "AuditorElector should be running");
        assertTrue(main.replicationWorker.isRunning(),
                "Replication worker should be running");

        main.shutdown();
        assertFalse(main.auditorElector.isRunning(),
                "AuditorElector should not be running");
        assertFalse(main.replicationWorker.isRunning(),
                "Replication worker should not be running");
    }

    /**
     * Test that, if an autorecovery looses its ZK connection/session it will
     * shutdown.
     */
    @Test
    void autoRecoverySessionLoss() throws Exception {
        /*
         * initialize three AutoRecovery instances.
         */
        AutoRecoveryMain main1 = new AutoRecoveryMain(confByIndex(0));
        AutoRecoveryMain main2 = new AutoRecoveryMain(confByIndex(1));
        AutoRecoveryMain main3 = new AutoRecoveryMain(confByIndex(2));

        /*
         * start main1, make sure all the components are started and main1 is
         * the current Auditor
         */
        ZKMetadataClientDriver zkMetadataClientDriver1 = startAutoRecoveryMain(main1);
        ZooKeeper zk1 = zkMetadataClientDriver1.getZk();

        // Wait until auditor gets elected
        for (int i = 0; i < 10; i++) {
            try {
                if (main1.auditorElector.getCurrentAuditor() != null) {
                    break;
                } else {
                    Thread.sleep(1000);
                }
            } catch (IOException e) {
                Thread.sleep(1000);
            }
        }
        BookieId currentAuditor = main1.auditorElector.getCurrentAuditor();
        assertNotNull(currentAuditor);
        Auditor auditor1 = main1.auditorElector.getAuditor();
        assertEquals(currentAuditor, BookieImpl.getBookieId(confByIndex(0)), "Current Auditor should be AR1");
        Awaitility.waitAtMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertNotNull(auditor1);
            assertTrue(auditor1.isRunning(), "Auditor of AR1 should be running");
        });


        /*
         * start main2 and main3
         */
        ZKMetadataClientDriver zkMetadataClientDriver2 = startAutoRecoveryMain(main2);
        ZooKeeper zk2 = zkMetadataClientDriver2.getZk();
        ZKMetadataClientDriver zkMetadataClientDriver3 = startAutoRecoveryMain(main3);
        ZooKeeper zk3 = zkMetadataClientDriver3.getZk();

        /*
         * make sure AR1 is still the current Auditor and AR2's and AR3's
         * auditors are not running.
         */
        assertEquals(currentAuditor, BookieImpl.getBookieId(confByIndex(0)), "Current Auditor should still be AR1");
        Awaitility.await().untilAsserted(() -> {
            assertTrue((main2.auditorElector.getAuditor() == null
                    || !main2.auditorElector.getAuditor().isRunning()), "AR2's Auditor should not be running");
            assertTrue((main3.auditorElector.getAuditor() == null
                    || !main3.auditorElector.getAuditor().isRunning()), "AR3's Auditor should not be running");
        });


        /*
         * expire zk2 and zk1 sessions.
         */
        zkUtil.expireSession(zk2);
        zkUtil.expireSession(zk1);

        /*
         * wait for some time for all the components of AR1 and AR2 are
         * shutdown.
         */
        for (int i = 0; i < 10; i++) {
            if (!main1.auditorElector.isRunning() && !main1.replicationWorker.isRunning()
                    && !main1.isAutoRecoveryRunning() && !main2.auditorElector.isRunning()
                    && !main2.replicationWorker.isRunning() && !main2.isAutoRecoveryRunning()) {
                break;
            }
            Thread.sleep(1000);
        }

        /*
         * the AR3 should be current auditor.
         */
        currentAuditor = main3.auditorElector.getCurrentAuditor();
        assertEquals(currentAuditor, BookieImpl.getBookieId(confByIndex(2)), "Current Auditor should be AR3");
        Awaitility.await().untilAsserted(() -> {
            assertNotNull(main3.auditorElector.getAuditor());
            assertTrue(main3.auditorElector.getAuditor().isRunning(), "Auditor of AR3 should be running");
        });

        Awaitility.await().untilAsserted(() -> {
            /*
             * since AR3 is current auditor, AR1's auditor should not be running
             * anymore.
             */
            assertFalse(auditor1.isRunning(), "AR1's auditor should not be running");

            /*
             * components of AR2 and AR3 should not be running since zk1 and zk2
             * sessions are expired.
             */
            assertFalse(main1.auditorElector.isRunning(), "Elector1 should have shutdown");
            assertFalse(main1.replicationWorker.isRunning(), "RW1 should have shutdown");
            assertFalse(main1.isAutoRecoveryRunning(), "AR1 should have shutdown");
            assertFalse(main2.auditorElector.isRunning(), "Elector2 should have shutdown");
            assertFalse(main2.replicationWorker.isRunning(), "RW2 should have shutdown");
            assertFalse(main2.isAutoRecoveryRunning(), "AR2 should have shutdown");
        });

    }

    /*
     * start autoRecoveryMain and make sure all its components are running and
     * myVote node is existing
     */
    ZKMetadataClientDriver startAutoRecoveryMain(AutoRecoveryMain autoRecoveryMain) throws Exception {
        autoRecoveryMain.start();
        ZKMetadataClientDriver metadataClientDriver = (ZKMetadataClientDriver) autoRecoveryMain.bkc
                .getMetadataClientDriver();
        TestUtils.assertEventuallyTrue("autoRecoveryMain components should be running",
                () -> autoRecoveryMain.auditorElector.isRunning()
                        && autoRecoveryMain.replicationWorker.isRunning() && autoRecoveryMain.isAutoRecoveryRunning());
        return metadataClientDriver;
    }
}
