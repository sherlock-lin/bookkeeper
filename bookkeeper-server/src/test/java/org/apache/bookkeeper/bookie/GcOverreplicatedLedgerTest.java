/**
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

package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.bookie.GarbageCollector.GarbageCleaner;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerTestCase;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.ZooDefs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test GC-overreplicated ledger.
 */
public class GcOverreplicatedLedgerTest extends LedgerManagerTestCase {

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ledgerManager = ledgerManagerFactory.newLedgerManager();
        activeLedgers = new SnapshotMap<Long, Boolean>();
    }

    public void initGcOverreplicatedLedgerTest(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        super(lmFactoryCls, 3);
    }

    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { HierarchicalLedgerManagerFactory.class } });
    }

    @MethodSource("configs")
    @ParameterizedTest
    public void gcOverreplicatedLedger(Class<? extends LedgerManagerFactory> lmFactoryCls) throws Exception {
        initGcOverreplicatedLedgerTest(lmFactoryCls);
        LedgerHandle lh = bkc.createLedger(2, 2, DigestType.MAC, "".getBytes());
        activeLedgers.put(lh.getId(), true);

        LedgerMetadata newLedgerMetadata = ledgerManager.readLedgerMetadata(lh.getId()).get().getValue();

        BookieId bookieNotInEnsemble = getBookieNotInEnsemble(newLedgerMetadata);
        ServerConfiguration bkConf = getBkConf(bookieNotInEnsemble);

        @Cleanup
        final MetadataBookieDriver metadataDriver = instantiateMetadataDriver(bkConf);
        @Cleanup
        final LedgerManagerFactory lmf = metadataDriver.getLedgerManagerFactory();
        @Cleanup
        final LedgerUnderreplicationManager lum = lmf.newLedgerUnderreplicationManager();

        assertFalse(lum.isLedgerBeingReplicated(lh.getId()));

        bkConf.setGcOverreplicatedLedgerWaitTime(10, TimeUnit.MILLISECONDS);

        lh.close();

        final CompactableLedgerStorage mockLedgerStorage = new MockLedgerStorage();
        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, mockLedgerStorage,
                bkConf, NullStatsLogger.INSTANCE);
        Thread.sleep(bkConf.getGcOverreplicatedLedgerWaitTimeMillis() + 1);
        garbageCollector.gc(new GarbageCleaner() {

            @Override
            public void clean(long ledgerId) {
                try {
                    mockLedgerStorage.deleteLedger(ledgerId);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }
        });

        assertFalse(lum.isLedgerBeingReplicated(lh.getId()));
        assertFalse(activeLedgers.containsKey(lh.getId()));
    }

    private static MetadataBookieDriver instantiateMetadataDriver(ServerConfiguration conf)
            throws BookieException {
        try {
            final String metadataServiceUriStr = conf.getMetadataServiceUri();
            final MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(URI.create(metadataServiceUriStr));
            driver.initialize(conf, NullStatsLogger.INSTANCE);
            return driver;
        } catch (MetadataException me) {
            throw new BookieException.MetadataStoreException("Failed to initialize metadata bookie driver", me);
        } catch (ConfigurationException e) {
            throw new BookieException.BookieIllegalOpException(e);
        }
    }

    @MethodSource("configs")
    @ParameterizedTest
    public void noGcOfLedger(Class<? extends LedgerManagerFactory> lmFactoryCls) throws Exception {
        initGcOverreplicatedLedgerTest(lmFactoryCls);
        LedgerHandle lh = bkc.createLedger(2, 2, DigestType.MAC, "".getBytes());
        activeLedgers.put(lh.getId(), true);

        LedgerMetadata newLedgerMetadata = ledgerManager.readLedgerMetadata(lh.getId()).get().getValue();
        BookieId address = null;
        SortedMap<Long, ? extends List<BookieId>> ensembleMap = newLedgerMetadata.getAllEnsembles();
        for (List<BookieId> ensemble : ensembleMap.values()) {
            address = ensemble.get(0);
        }
        ServerConfiguration bkConf = getBkConf(address);
        bkConf.setGcOverreplicatedLedgerWaitTime(10, TimeUnit.MILLISECONDS);

        lh.close();

        final CompactableLedgerStorage mockLedgerStorage = new MockLedgerStorage();
        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, mockLedgerStorage,
                bkConf, NullStatsLogger.INSTANCE);
        Thread.sleep(bkConf.getGcOverreplicatedLedgerWaitTimeMillis() + 1);
        garbageCollector.gc(new GarbageCleaner() {

            @Override
            public void clean(long ledgerId) {
                try {
                    mockLedgerStorage.deleteLedger(ledgerId);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }
        });

        assertTrue(activeLedgers.containsKey(lh.getId()));
    }

    @MethodSource("configs")
    @ParameterizedTest
    public void noGcIfLedgerBeingReplicated(Class<? extends LedgerManagerFactory> lmFactoryCls) throws Exception {
        initGcOverreplicatedLedgerTest(lmFactoryCls);
        LedgerHandle lh = bkc.createLedger(2, 2, DigestType.MAC, "".getBytes());
        activeLedgers.put(lh.getId(), true);

        LedgerMetadata newLedgerMetadata = ledgerManager.readLedgerMetadata(lh.getId()).get().getValue();
        BookieId bookieNotInEnsemble = getBookieNotInEnsemble(newLedgerMetadata);
        ServerConfiguration bkConf = getBkConf(bookieNotInEnsemble);
        bkConf.setGcOverreplicatedLedgerWaitTime(10, TimeUnit.MILLISECONDS);

        lh.close();

        ZkLedgerUnderreplicationManager.acquireUnderreplicatedLedgerLock(
            zkc,
            ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf),
            lh.getId(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE);

        final CompactableLedgerStorage mockLedgerStorage = new MockLedgerStorage();
        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, mockLedgerStorage,
                bkConf, NullStatsLogger.INSTANCE);
        Thread.sleep(bkConf.getGcOverreplicatedLedgerWaitTimeMillis() + 1);
        garbageCollector.gc(new GarbageCleaner() {

            @Override
            public void clean(long ledgerId) {
                try {
                    mockLedgerStorage.deleteLedger(ledgerId);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }
        });

        assertTrue(activeLedgers.containsKey(lh.getId()));
    }

    private BookieId getBookieNotInEnsemble(LedgerMetadata ledgerMetadata) throws Exception {
        List<BookieId> allAddresses = Lists.newArrayList();
        allAddresses.addAll(bookieAddresses());
        SortedMap<Long, ? extends List<BookieId>> ensembles = ledgerMetadata.getAllEnsembles();
        for (List<BookieId> fragmentEnsembles : ensembles.values()) {
            allAddresses.removeAll(fragmentEnsembles);
        }
        assertEquals(1, allAddresses.size());
        return allAddresses.get(0);
    }
}
