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
package org.apache.bookkeeper.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the ZK ledger id generator.
 */
class TestZkLedgerIdGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TestZkLedgerIdGenerator.class);

    ZooKeeperUtil zkutil;
    ZooKeeper zk;

    LedgerIdGenerator ledgerIdGenerator;


    @BeforeEach
    @BeforeEach
    void setUp() throws Exception {
        LOG.info("Setting up test");

        zkutil = new ZooKeeperUtil();
        zkutil.startCluster();
        zk = zkutil.getZooKeeperClient();

        ledgerIdGenerator = new ZkLedgerIdGenerator(zk,
                "/test-zk-ledger-id-generator", "idgen", ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }


    @AfterEach
    @AfterEach
    void tearDown() throws Exception {
        LOG.info("Tearing down test");
        ledgerIdGenerator.close();
        zk.close();
        zkutil.killCluster();
    }

    @Test
    void generateLedgerId() throws Exception {
        // Create *nThread* threads each generate *nLedgers* ledger id,
        // and then check there is no identical ledger id.
        final int nThread = 2;
        final int nLedgers = 2000;
        final CountDownLatch countDownLatch = new CountDownLatch(nThread * nLedgers);

        final AtomicInteger errCount = new AtomicInteger(0);
        final ConcurrentLinkedQueue<Long> ledgerIds = new ConcurrentLinkedQueue<Long>();
        final GenericCallback<Long> cb = new GenericCallback<Long>() {
            @Override
            public void operationComplete(int rc, Long result) {
                if (Code.OK.intValue() == rc) {
                    ledgerIds.add(result);
                } else {
                    errCount.incrementAndGet();
                }
                countDownLatch.countDown();
            }
        };

        long start = System.currentTimeMillis();

        for (int i = 0; i < nThread; i++) {
            new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < nLedgers; j++) {
                        ledgerIdGenerator.generateLedgerId(cb);
                    }
                }
            }.start();
        }

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS),
                "Wait ledger id generation threads to stop timeout : ");
        LOG.info("Number of generated ledger id: {}, time used: {}", ledgerIds.size(),
                System.currentTimeMillis() - start);
        assertEquals(0, errCount.get(), "Error occur during ledger id generation : ");

        Set<Long> ledgers = new HashSet<Long>();
        while (!ledgerIds.isEmpty()) {
            Long ledger = ledgerIds.poll();
            assertNotNull(ledger, "Generated ledger id is null : ");
            assertFalse(ledgers.contains(ledger), "Ledger id [" + ledger + "] conflict : ");
            ledgers.add(ledger);
        }
    }

}
