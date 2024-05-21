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
package org.apache.bookkeeper.meta.zk;

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import lombok.Cleanup;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit test of {@link ZKMetadataDriverBase}.
 */
@ExtendWith(MockitoExtension.class)
class ZKMetadataDriverBaseTest extends ZKMetadataDriverTestBase {

    private ZKMetadataDriverBase driver;
    private RetryPolicy retryPolicy;

    @BeforeEach
    void setup() throws Exception {
        super.setup(new ClientConfiguration());
        driver = mock(ZKMetadataDriverBase.class, CALLS_REAL_METHODS);
        retryPolicy = mock(RetryPolicy.class);
    }

    @AfterEach
    public void teardown() {
        super.teardown();
    }

    @Test
    void initialize() throws Exception {
        driver.initialize(
            conf, NullStatsLogger.INSTANCE, retryPolicy, Optional.empty());

        assertEquals(
            "/path/to/ledgers",
            driver.ledgersRootPath);
        assertTrue(driver.ownZKHandle);

        String readonlyPath = "/path/to/ledgers/" + AVAILABLE_NODE + "/" + READONLY;
        assertSame(mockZkc, driver.zk);

        ZooKeeperClient.newBuilder();
        verify(mockZkBuilder, times(1)).build();
        verify(mockZkc, times(1))
            .exists(eq(readonlyPath), eq(false));
        assertNotNull(driver.layoutManager);
        assertNull(driver.lmFactory);

        driver.close();

        verify(mockZkc, times(1)).close(5000);
        assertNull(driver.zk);
    }

    @Test
    void initializeExternalZooKeeper() throws Exception {
        ZooKeeperClient anotherZk = mock(ZooKeeperClient.class);

        driver.initialize(
            conf, NullStatsLogger.INSTANCE, retryPolicy, Optional.of(anotherZk));

        assertEquals(
            "/ledgers",
            driver.ledgersRootPath);
        assertFalse(driver.ownZKHandle);

        String readonlyPath = "/path/to/ledgers/" + AVAILABLE_NODE;
        assertSame(anotherZk, driver.zk);

        ZooKeeperClient.newBuilder();
        verify(mockZkBuilder, times(0)).build();
        verify(mockZkc, times(0))
            .exists(eq(readonlyPath), eq(false));
        assertNotNull(driver.layoutManager);
        assertNull(driver.lmFactory);

        driver.close();

        verify(mockZkc, times(0)).close();
        assertNotNull(driver.zk);
    }

    @Test
    void getLedgerManagerFactory() throws Exception {
        driver.initialize(
            conf, NullStatsLogger.INSTANCE, retryPolicy, Optional.empty());

        @Cleanup
        MockedStatic<AbstractZkLedgerManagerFactory> abstractZkLedgerManagerFactoryMockedStatic =
                mockStatic(AbstractZkLedgerManagerFactory.class);
        LedgerManagerFactory factory = mock(LedgerManagerFactory.class);
        abstractZkLedgerManagerFactoryMockedStatic.when(() ->
                        AbstractZkLedgerManagerFactory.newLedgerManagerFactory(same(conf), same(driver.layoutManager)))
                .thenReturn(factory);

        assertSame(factory, driver.getLedgerManagerFactory());
        assertSame(factory, driver.lmFactory);

        AbstractZkLedgerManagerFactory.newLedgerManagerFactory(
            same(conf),
            same(driver.layoutManager));

        driver.close();
        verify(factory, times(1)).close();
        assertNull(driver.lmFactory);
    }

}
