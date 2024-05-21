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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.junit.jupiter.api.Test;

/**
 * Unit test the static methods of {@link ZKMetadataDriverBase}.
 */
class ZKMetadataDriverBaseStaticTest {

    @Test
    void getZKServersFromServiceUri() {
        String uriStr = "zk://server1;server2;server3/ledgers";
        URI uri = URI.create(uriStr);

        String zkServers = ZKMetadataDriverBase.getZKServersFromServiceUri(uri);
        assertEquals(
            "server1,server2,server3",
            zkServers);

        uriStr = "zk://server1,server2,server3/ledgers";
        uri = URI.create(uriStr);
        zkServers = ZKMetadataDriverBase.getZKServersFromServiceUri(uri);
        assertEquals(
            "server1,server2,server3",
            zkServers);
    }

    @Test
    void resolveLedgerManagerFactoryNullUri() {
        assertThrows(NullPointerException.class, () -> {
            ZKMetadataDriverBase.resolveLedgerManagerFactory(null);
        });
    }

    @Test
    void resolveLedgerManagerFactoryNullScheme() {
        assertThrows(NullPointerException.class, () -> {
            ZKMetadataDriverBase.resolveLedgerManagerFactory(URI.create("//127.0.0.1/ledgers"));
        });
    }

    @Test
    void resolveLedgerManagerFactoryUnknownScheme() {
        assertThrows(IllegalArgumentException.class, () -> {
            ZKMetadataDriverBase.resolveLedgerManagerFactory(URI.create("unknown://127.0.0.1/ledgers"));
        });
    }

    @Test
    void resolveLedgerManagerFactoryUnspecifiedLayout() {
        assertNull(ZKMetadataDriverBase.resolveLedgerManagerFactory(
                URI.create("zk://127.0.0.1/ledgers")));
    }

    @Test
    void resolveLedgerManagerFactoryNullLayout() {
        assertNull(ZKMetadataDriverBase.resolveLedgerManagerFactory(
                URI.create("zk+null://127.0.0.1/ledgers")));
    }

    @SuppressWarnings("deprecation")
    @Test
    void resolveLedgerManagerFactoryFlat() {
        assertEquals(
            org.apache.bookkeeper.meta.FlatLedgerManagerFactory.class,
            ZKMetadataDriverBase.resolveLedgerManagerFactory(
                URI.create("zk+flat://127.0.0.1/ledgers"))
        );
    }

    @SuppressWarnings("deprecation")
    @Test
    void resolveLedgerManagerFactoryMs() {
        assertEquals(
            org.apache.bookkeeper.meta.MSLedgerManagerFactory.class,
            ZKMetadataDriverBase.resolveLedgerManagerFactory(
                URI.create("zk+ms://127.0.0.1/ledgers"))
        );
    }

    @Test
    void resolveLedgerManagerFactoryHierarchical() {
        assertEquals(
            HierarchicalLedgerManagerFactory.class,
            ZKMetadataDriverBase.resolveLedgerManagerFactory(
                URI.create("zk+hierarchical://127.0.0.1/ledgers"))
        );
    }

    @Test
    void resolveLedgerManagerFactoryLongHierarchical() {
        assertEquals(
            LongHierarchicalLedgerManagerFactory.class,
            ZKMetadataDriverBase.resolveLedgerManagerFactory(
                URI.create("zk+longhierarchical://127.0.0.1/ledgers"))
        );
    }

    @Test
    void resolveLedgerManagerFactoryUnknownLedgerManagerFactory() {
        assertThrows(IllegalArgumentException.class, () -> {
            ZKMetadataDriverBase.resolveLedgerManagerFactory(
                    URI.create("zk+unknown://127.0.0.1/ledgers"));
        });
    }
}
