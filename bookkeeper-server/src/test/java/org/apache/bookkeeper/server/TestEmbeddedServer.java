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

package org.apache.bookkeeper.server;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import lombok.Cleanup;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LegacyCookieValidation;
import org.apache.bookkeeper.bookie.UncleanShutdownDetectionImpl;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorWithOomHandler;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver.NullLedgerManagerFactory;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver.NullRegistrationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit test of {@link EmbeddedServer}.
 */
@ExtendWith(MockitoExtension.class)
class TestEmbeddedServer {

    static class TestComponent extends ServerLifecycleComponent {

        public TestComponent(BookieConfiguration conf, StatsLogger statsLogger) {
            super("test-component", conf, statsLogger);
        }

        @Override
        protected void doStart() {
        }

        @Override
        protected void doStop() {
        }

        @Override
        protected void doClose() throws IOException {
        }

    }

    @Test
    void buildBookieServer() throws Exception {
        @Cleanup
        MockedStatic<BookieResources> bookieResourcesMockedStatic = mockStatic(BookieResources.class,
                CALLS_REAL_METHODS);
        bookieResourcesMockedStatic.when(() ->
                BookieResources.createMetadataDriver(any(), any())).thenReturn(new NullMetadataBookieDriver());
        bookieResourcesMockedStatic.when(() ->
                BookieResources.createAllocator(any())).thenReturn(mock(ByteBufAllocatorWithOomHandler.class));

        ServerConfiguration serverConf = new ServerConfiguration()
            .setAllowLoopback(true)
            .setAutoRecoveryDaemonEnabled(false)
            .setHttpServerEnabled(false)
            .setExtraServerComponents(new String[] { TestComponent.class.getName() });
        BookieConfiguration conf = new BookieConfiguration(serverConf);

        @Cleanup
        MockedStatic<LegacyCookieValidation> legacyCookieValidationMockedStatic =
                mockStatic(LegacyCookieValidation.class);
        legacyCookieValidationMockedStatic.when(() -> LegacyCookieValidation.newLegacyCookieValidation(any(), any()))
                .thenReturn(mock(LegacyCookieValidation.class));

        @Cleanup
        MockedStatic<BookieImpl> bookieMockedStatic = mockStatic(BookieImpl.class, CALLS_REAL_METHODS);
        bookieMockedStatic.when(() -> BookieImpl.newBookieImpl(any(), any(), any(), any(), any(), any(), any(),
                any(), any())).thenReturn(mock(BookieImpl.class));

        BookieServer mockServer = mock(BookieServer.class);

        BookieSocketAddress bookieAddress = new BookieSocketAddress("127.0.0.1", 1281);
        when(mockServer.getLocalAddress()).thenReturn(bookieAddress);

        @Cleanup
        MockedStatic<BookieServer> bookieServerMockedStatic = mockStatic(BookieServer.class);
        bookieServerMockedStatic.when(() -> BookieServer.newBookieServer(any(), any(), any(), any(), any()))
                .thenReturn(mockServer);

        EmbeddedServer server = EmbeddedServer.builder(conf).build();
        LifecycleComponentStack stack = server.getLifecycleComponentStack();
        assertEquals(7, stack.getNumComponents());
        assertTrue(stack.getComponent(6) instanceof TestComponent);

        stack.start();
        verify(mockServer, times(1)).start();

        stack.stop();

        stack.close();
        verify(mockServer, times(1)).shutdown();
    }

    @Test
    void buildBookieServerCustomComponents() throws Exception {

        ServerConfiguration serverConf = new ServerConfiguration()
                .setAllowLoopback(true)
                .setAutoRecoveryDaemonEnabled(false)
                .setHttpServerEnabled(false)
                .setExtraServerComponents(new String[]{TestComponent.class.getName()});
        BookieConfiguration conf = new BookieConfiguration(serverConf);

        StatsProvider statsProvider = new NullStatsProvider();
        StatsLogger rootStatsLogger = statsProvider.getStatsLogger("");
        RegistrationManager registrationManager = new NullRegistrationManager();
        LedgerManagerFactory ledgerManagerFactory = new NullLedgerManagerFactory();

        DiskChecker diskChecker = BookieResources.createDiskChecker(serverConf);

        LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(
                conf.getServerConf(), diskChecker, rootStatsLogger.scope(LD_LEDGER_SCOPE));

        LedgerDirsManager indexDirsManager = BookieResources.createIndexDirsManager(
                conf.getServerConf(), diskChecker, rootStatsLogger.scope(LD_INDEX_SCOPE), ledgerDirsManager);

        UncleanShutdownDetectionImpl uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);

        ByteBufAllocatorWithOomHandler byteBufFromResources = mock(ByteBufAllocatorWithOomHandler.class);
        ByteBufAllocatorWithOomHandler byteBuf = mock(ByteBufAllocatorWithOomHandler.class);

        @Cleanup
        MockedStatic<BookieResources> bookieResourcesMockedStatic = mockStatic(BookieResources.class);
        bookieResourcesMockedStatic.when(() ->
                BookieResources.createMetadataDriver(any(), any())).thenReturn(new NullMetadataBookieDriver());
        bookieResourcesMockedStatic.when(() ->
                BookieResources.createAllocator(any())).thenReturn(byteBufFromResources);

        @Cleanup
        MockedStatic<LegacyCookieValidation> legacyCookieValidationMockedStatic =
                mockStatic(LegacyCookieValidation.class);
        legacyCookieValidationMockedStatic.when(() -> LegacyCookieValidation.newLegacyCookieValidation(any(), any()))
                .thenReturn(mock(LegacyCookieValidation.class));

        @Cleanup
        MockedStatic<BookieImpl> bookieMockedStatic = mockStatic(BookieImpl.class, CALLS_REAL_METHODS);
        bookieMockedStatic.when(() -> BookieImpl.newBookieImpl(any(), any(), any(), any(), any(), any(), any(), any(),
                any())).thenReturn(mock(BookieImpl.class));

        BookieServer mockServer = mock(BookieServer.class);

        @Cleanup
        MockedStatic<BookieServer> bookieServerMockedStatic = mockStatic(BookieServer.class);
        bookieServerMockedStatic.when(() -> BookieServer.newBookieServer(any(), any(), any(), any(), any()))
                .thenReturn(mockServer);

        BookieSocketAddress bookieAddress = new BookieSocketAddress("127.0.0.1", 1281);
        when(mockServer.getLocalAddress()).thenReturn(bookieAddress);

        EmbeddedServer server = EmbeddedServer.builder(conf)
                .statsProvider(statsProvider)
                .registrationManager(registrationManager)
                .ledgerManagerFactory(ledgerManagerFactory)
                .diskChecker(diskChecker)
                .ledgerDirsManager(ledgerDirsManager)
                .indexDirsManager(indexDirsManager)
                .allocator(byteBuf)
                .uncleanShutdownDetection(uncleanShutdownDetection)
                .build();

        assertSame(statsProvider, server.getStatsProvider());
        assertSame(registrationManager, server.getRegistrationManager());
        assertSame(ledgerManagerFactory, server.getLedgerManagerFactory());
        assertSame(diskChecker, server.getDiskChecker());
        assertSame(ledgerDirsManager, server.getLedgerDirsManager());
        assertSame(indexDirsManager, server.getIndexDirsManager());

        LifecycleComponentStack stack = server.getLifecycleComponentStack();
        assertEquals(3, stack.getNumComponents());
        assertTrue(stack.getComponent(2) instanceof TestComponent);

        stack.start();
        verify(mockServer, times(1)).start();

        stack.stop();

        stack.close();
        verify(mockServer, times(1)).shutdown();
    }

    @Test
    void ignoreExtraServerComponentsStartupFailures() throws Exception {
        @Cleanup
        MockedStatic<BookieResources> bookieResourcesMockedStatic = mockStatic(BookieResources.class,
                CALLS_REAL_METHODS);
        bookieResourcesMockedStatic.when(() ->
                BookieResources.createMetadataDriver(any(), any())).thenReturn(new NullMetadataBookieDriver());

        ServerConfiguration serverConf = new ServerConfiguration()
            .setAllowLoopback(true)
            .setAutoRecoveryDaemonEnabled(false)
            .setHttpServerEnabled(false)
            .setExtraServerComponents(new String[] { "bad-server-component"})
            .setIgnoreExtraServerComponentsStartupFailures(true);
        BookieConfiguration conf = new BookieConfiguration(serverConf);

        @Cleanup
        MockedStatic<LegacyCookieValidation> legacyCookieValidationMockedStatic =
                mockStatic(LegacyCookieValidation.class);
        legacyCookieValidationMockedStatic.when(() -> LegacyCookieValidation.newLegacyCookieValidation(any(), any()))
                .thenReturn(mock(LegacyCookieValidation.class));

        @Cleanup
        MockedStatic<BookieImpl> bookieMockedStatic = mockStatic(BookieImpl.class, CALLS_REAL_METHODS);
        bookieMockedStatic.when(() -> BookieImpl.newBookieImpl(any(), any(), any(), any(), any(), any(), any(), any(),
                any())).thenReturn(mock(BookieImpl.class));

        BookieServer mockServer = mock(BookieServer.class);

        @Cleanup
        MockedStatic<BookieServer> bookieServerMockedStatic = mockStatic(BookieServer.class);
        bookieServerMockedStatic.when(() -> BookieServer.newBookieServer(any(), any(), any(), any(), any()))
                .thenReturn(mockServer);

        BookieSocketAddress bookieAddress = new BookieSocketAddress("127.0.0.1", 1281);
        when(mockServer.getLocalAddress()).thenReturn(bookieAddress);

        LifecycleComponentStack stack = EmbeddedServer.builder(conf).build().getLifecycleComponentStack();
        assertEquals(6, stack.getNumComponents());

        stack.start();
        verify(mockServer, times(1)).start();

        stack.stop();

        stack.close();
        verify(mockServer, times(1)).shutdown();
    }

    @Test
    void extraServerComponentsStartupFailures() throws Exception {
        @Cleanup
        MockedStatic<BookieResources> bookieResourcesMockedStatic = mockStatic(BookieResources.class,
                CALLS_REAL_METHODS);
        bookieResourcesMockedStatic.when(() ->
                BookieResources.createMetadataDriver(any(), any())).thenReturn(new NullMetadataBookieDriver());

        ServerConfiguration serverConf = new ServerConfiguration()
            .setAllowLoopback(true)
            .setAutoRecoveryDaemonEnabled(false)
            .setHttpServerEnabled(false)
            .setExtraServerComponents(new String[] { "bad-server-component"})
            .setIgnoreExtraServerComponentsStartupFailures(false);
        BookieConfiguration conf = new BookieConfiguration(serverConf);

        @Cleanup
        MockedStatic<LegacyCookieValidation> legacyCookieValidationMockedStatic =
                mockStatic(LegacyCookieValidation.class);
        legacyCookieValidationMockedStatic.when(() -> LegacyCookieValidation.newLegacyCookieValidation(any(), any()))
                .thenReturn(mock(LegacyCookieValidation.class));

        @Cleanup
        MockedStatic<BookieImpl> bookieMockedStatic = mockStatic(BookieImpl.class, CALLS_REAL_METHODS);
        bookieMockedStatic.when(() -> BookieImpl.newBookieImpl(any(), any(), any(), any(), any(), any(), any(), any(),
                any())).thenReturn(mock(BookieImpl.class));

        BookieServer mockServer = mock(BookieServer.class);

        @Cleanup
        MockedStatic<BookieServer> bookieServerMockedStatic = mockStatic(BookieServer.class);
        bookieServerMockedStatic.when(() -> BookieServer.newBookieServer(any(), any(), any(), any(), any()))
                .thenReturn(mockServer);

        try {
            EmbeddedServer.builder(conf).build().getLifecycleComponentStack();
            fail("Should fail to start bookie server if `ignoreExtraServerComponentsStartupFailures` is set to false");
        } catch (RuntimeException re) {
            assertTrue(re.getCause() instanceof ClassNotFoundException);
        }
    }
}
