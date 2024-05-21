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
package org.apache.bookkeeper.util;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.getName;

import java.io.IOException;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test ZooKeeper utilities.
 */
class TestZkUtils {

    private static final Logger logger = LoggerFactory.getLogger(TestZkUtils.class);

    // ZooKeeper related variables
    protected ZooKeeperUtil zkUtil = new ZooKeeperUtil();

    @BeforeEach
    @BeforeEach
    void setUp() throws Exception {
        logger.info("Setting up test {}.", getName());
        zkUtil.startCluster();
    }

    @AfterEach
    @AfterEach
    void tearDown() throws Exception {
        zkUtil.killCluster();
        logger.info("Teared down test {}.", getName());
    }

    @Test
    void asyncCreateAndDeleteFullPathOptimistic() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zkc = new ZooKeeper(zkUtil.getZooKeeperConnectString(), 10000, null);
        /*
         * "/ledgers/available" is already created in ZooKeeperUtil.startServer
         */
        String ledgerZnodePath = "/ledgers/000/000/000/001";
        ZkUtils.createFullPathOptimistic(zkc, ledgerZnodePath, "data".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        assertTrue(null != zkc.exists(ledgerZnodePath, false), ledgerZnodePath + " zNode should exist");

        ledgerZnodePath = "/ledgers/000/000/000/002";
        ZkUtils.createFullPathOptimistic(zkc, ledgerZnodePath, "data".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        assertTrue(null != zkc.exists(ledgerZnodePath, false), ledgerZnodePath + " zNode should exist");

        ZkUtils.deleteFullPathOptimistic(zkc, ledgerZnodePath, -1);
        assertTrue(null == zkc.exists(ledgerZnodePath, false),
                ledgerZnodePath + " zNode should not exist, since it is deleted");

        ledgerZnodePath = "/ledgers/000/000/000/001";
        assertTrue(null != zkc.exists(ledgerZnodePath, false), ledgerZnodePath + " zNode should exist");
        ZkUtils.deleteFullPathOptimistic(zkc, ledgerZnodePath, -1);
        assertTrue(null == zkc.exists(ledgerZnodePath, false),
                ledgerZnodePath + " zNode should not exist, since it is deleted");
        assertTrue(null == zkc.exists(ledgerZnodePath, false),
                "/ledgers/000" + " zNode should not exist, since it should be deleted recursively");
    }
}
