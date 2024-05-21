/*
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
 */
package org.apache.bookkeeper.discover;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test of {@link RegistrationManager}.
 */
class TestZkRegistrationManager {

    private ZooKeeperCluster localZkServer;
    private ZooKeeper zkc;

    @BeforeEach
    void setup() throws Exception {
        localZkServer = new ZooKeeperUtil();
        localZkServer.startCluster();
    }

    @AfterEach
    void teardown() throws Exception {
        localZkServer.stopCluster();
    }

    @Test
    void prepareFormat() throws Exception {
        try {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setMetadataServiceUri("zk+hierarchical://localhost:2181/test/ledgers");
            zkc = localZkServer.getZooKeeperClient();
            ZKRegistrationManager zkRegistrationManager = new ZKRegistrationManager(conf, zkc);
            zkRegistrationManager.prepareFormat();
            assertNotNull(zkc.exists("/test/ledgers", false));
        } finally {
            if (zkc != null) {
                zkc.close();
            }
        }
    }

}
