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
package org.apache.bookkeeper.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieConnectionPeer;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.ClientConnectionPeer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test authentication.
 */
public class TestAuth extends BookKeeperClusterTestCase {
    static final Logger LOG = LoggerFactory.getLogger(TestAuth.class);
    public static final String TEST_AUTH_PROVIDER_PLUGIN_NAME = "TestAuthProviderPlugin";
    private static final byte[] PASSWD = "testPasswd".getBytes();
    private static final byte[] ENTRY = "TestEntry".getBytes();

    private static final byte[] SUCCESS_RESPONSE = {1};
    private static final byte[] FAILURE_RESPONSE = {2};
    private static final byte[] PAYLOAD_MESSAGE = {3};

    enum ProtocolVersion {
        ProtocolV2, ProtocolV3
    }

    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                { ProtocolVersion.ProtocolV2 },
                { ProtocolVersion.ProtocolV3 },
        });
    }

    private ProtocolVersion protocolVersion;

    public void initTestAuth(ProtocolVersion protocolVersion) {
        super(0); // start them later when auth providers are configured
        this.protocolVersion = protocolVersion;
    }

    protected ClientConfiguration newClientConfiguration() {
        ClientConfiguration conf = super.newClientConfiguration();
        conf.setUseV2WireProtocol(protocolVersion == ProtocolVersion.ProtocolV2);
        return conf;
    }

    // we pass in ledgerId because the method may throw exceptions
    private void connectAndWriteToBookie(ClientConfiguration conf, AtomicLong ledgerWritten)
            throws Exception {
        LOG.info("Connecting to bookie");
        try (BookKeeper bkc = new BookKeeper(conf, zkc);
            LedgerHandle l = bkc.createLedger(1, 1, DigestType.CRC32,
                    PASSWD)) {
            ledgerWritten.set(l.getId());
            l.addEntry(ENTRY);
        }
    }

    /**
     * check if the entry exists. Restart the bookie to allow
     * access
     */
    private int entryCount(long ledgerId, ServerConfiguration bookieConf,
                           ClientConfiguration clientConf) throws Exception {
        LOG.info("Counting entries in {}", ledgerId);
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        restartBookies(c -> {
                c.setBookieAuthProviderFactoryClass(
                        AlwaysSucceedBookieAuthProviderFactory.class.getName());
                return c;
            });

        int count = 0;
        try (BookKeeper bkc = new BookKeeper(clientConf, zkc);
            LedgerHandle lh = bkc.openLedger(ledgerId, DigestType.CRC32,
                                         PASSWD)) {
            if (lh.getLastAddConfirmed() < 0) {
                return 0;
            }
            Enumeration<LedgerEntry> e = lh.readEntries(0, lh.getLastAddConfirmed());
            while (e.hasMoreElements()) {
                count++;
                assertTrue(Arrays.equals(e.nextElement().getEntry(), ENTRY),
                        "Should match what we wrote");
            }
        }
        return count;
    }

    /**
     * Test an connection will authorize with a single message
     * to the server and a single response.
     */
    @MethodSource("configs")
    @ParameterizedTest
    public void singleMessageAuth(ProtocolVersion protocolVersion) throws Exception {
        initTestAuth(protocolVersion);
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
            AlwaysSucceedBookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
            SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        connectAndWriteToBookie(clientConf, ledgerId); // should succeed

        assertFalse(ledgerId.get() == -1);
        assertEquals(1, entryCount(ledgerId.get(), bookieConf, clientConf), "Should have entry");
    }

    @MethodSource("configs")
    @ParameterizedTest
    public void closeMethodCalledOnAuthProvider(ProtocolVersion protocolVersion) throws Exception {
        initTestAuth(protocolVersion);
        LogCloseCallsBookieAuthProviderFactory.closeCountersOnFactory.set(0);
        LogCloseCallsBookieAuthProviderFactory.closeCountersOnConnections.set(0);
        LogCloseCallsBookieAuthProviderFactory.initCountersOnFactory.set(0);
        LogCloseCallsBookieAuthProviderFactory.initCountersOnConnections.set(0);
        LogCloseCallsClientAuthProviderFactory.initCountersOnFactory.set(0);
        LogCloseCallsClientAuthProviderFactory.closeCountersOnFactory.set(0);

        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                LogCloseCallsBookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                LogCloseCallsClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        connectAndWriteToBookie(clientConf, ledgerId); // should succeed

        assertFalse(ledgerId.get() == -1);
        assertEquals(1, entryCount(ledgerId.get(), bookieConf, clientConf), "Should have entry");

        stopAllBookies();

        assertEquals(LogCloseCallsBookieAuthProviderFactory.initCountersOnConnections.get(),
            LogCloseCallsBookieAuthProviderFactory.closeCountersOnConnections.get());
        assertTrue(LogCloseCallsBookieAuthProviderFactory.initCountersOnConnections.get() > 0);

        assertEquals(1, LogCloseCallsBookieAuthProviderFactory.initCountersOnFactory.get());
        assertEquals(1, LogCloseCallsBookieAuthProviderFactory.closeCountersOnFactory.get());

        assertEquals(LogCloseCallsClientAuthProviderFactory.initCountersOnConnections.get(),
            LogCloseCallsClientAuthProviderFactory.closeCountersOnConnections.get());
        assertTrue(LogCloseCallsClientAuthProviderFactory.initCountersOnConnections.get() > 0);

        assertEquals(1, LogCloseCallsClientAuthProviderFactory.initCountersOnFactory.get());
        assertEquals(1, LogCloseCallsClientAuthProviderFactory.closeCountersOnFactory.get());

    }

    /**
     * Test that when the bookie provider sends a failure message
     * the client will not be able to write.
     */
    @MethodSource("configs")
    @ParameterizedTest
    public void singleMessageAuthFailure(ProtocolVersion protocolVersion) throws Exception {
        initTestAuth(protocolVersion);
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                AlwaysFailBookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId); // should fail
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // client shouldnt be able to find enough bookies to
            // write
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals(0, entryCount(ledgerId.get(), bookieConf, clientConf), "Shouldn't have entry");
    }

    /**
     * Test that authentication works when the providers
     * exchange multiple messages.
     */
    @MethodSource("configs")
    @ParameterizedTest
    public void multiMessageAuth(ProtocolVersion protocolVersion) throws Exception {
        initTestAuth(protocolVersion);
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                SucceedAfter3BookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        AtomicLong ledgerId = new AtomicLong(-1);
        startAndStoreBookie(bookieConf);
        connectAndWriteToBookie(clientConf, ledgerId); // should succeed

        assertFalse(ledgerId.get() == -1);
        assertEquals(1, entryCount(ledgerId.get(), bookieConf, clientConf), "Should have entry");
    }

    /**
     * Test that when the bookie provider sends a failure message
     * the client will not be able to write.
     */
    @MethodSource("configs")
    @ParameterizedTest
    public void multiMessageAuthFailure(ProtocolVersion protocolVersion) throws Exception {
        initTestAuth(protocolVersion);
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                FailAfter3BookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId); // should fail
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // bookie should have sent a negative response before
            // breaking the conneciton
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals(0, entryCount(ledgerId.get(), bookieConf, clientConf), "Shouldn't have entry");
    }

    /**
     * Test that when the bookie and the client have a different
     * plugin configured, no messages will get through.
     */
    @MethodSource("configs")
    @ParameterizedTest
    public void differentPluginFailure(ProtocolVersion protocolVersion) throws Exception {
        initTestAuth(protocolVersion);
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                DifferentPluginBookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);
        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId); // should fail
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // bookie should have sent a negative response before
            // breaking the conneciton
            assertEquals(ProtocolVersion.ProtocolV3, protocolVersion);
        } catch (BKException.BKNotEnoughBookiesException nebe) {
            // With V2 we don't get the authorization error, but rather just
            // fail to write to bookies.
            assertEquals(ProtocolVersion.ProtocolV2, protocolVersion);
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals(0, entryCount(ledgerId.get(), bookieConf, clientConf), "Shouldn't have entry");
    }

    /**
     * Test that when the plugin class does exist, but
     * doesn't implement the interface, we fail predictably.
     */
    @MethodSource("configs")
    @ParameterizedTest
    public void existantButNotValidPlugin(ProtocolVersion protocolVersion) throws Exception {
        initTestAuth(protocolVersion);
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                "java.lang.String");

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                "java.lang.String");
        try {
            startAndStoreBookie(bookieConf);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertTrue(e.getMessage().contains("not "
                            + BookieAuthProvider.Factory.class.getName()),
                    "Wrong exception thrown");
        }

        try {
            BookKeeper bkc = new BookKeeper(clientConf, zkc);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertTrue(e.getMessage().contains("not "
                            + ClientAuthProvider.Factory.class.getName()),
                    "Wrong exception thrown");
        }
    }

    /**
     * Test that when the plugin class does not exist,
     * the bookie will not start and the client will
     * break.
     */
    @MethodSource("configs")
    @ParameterizedTest
    public void nonExistantPlugin(ProtocolVersion protocolVersion) throws Exception {
        initTestAuth(protocolVersion);
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                "NonExistantClassNameForTestingAuthPlugins");

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                "NonExistantClassNameForTestingAuthPlugins");
        try {
            startAndStoreBookie(bookieConf);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertEquals(ClassNotFoundException.class, e.getCause().getClass(), "Wrong exception thrown");
        }

        try {
            BookKeeper bkc = new BookKeeper(clientConf, zkc);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertEquals(ClassNotFoundException.class, e.getCause().getClass(), "Wrong exception thrown");
        }
    }

    /**
     * Test that when the plugin on the bookie crashes, the client doesn't
     * hang also, but it cannot write in any case.
     */
    @MethodSource("configs")
    @ParameterizedTest
    public void crashDuringAuth(ProtocolVersion protocolVersion) throws Exception {
        initTestAuth(protocolVersion);
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                CrashAfter3BookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId);
            fail("Shouldn't get this far");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // bookie won't respond, request will timeout, and then
            // we wont be able to find a replacement
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals(0, entryCount(ledgerId.get(), bookieConf, clientConf), "Shouldn't have entry");
    }

    /**
     * Test that when a bookie simply stops replying during auth, the client doesn't
     * hang also, but it cannot write in any case.
     */
    @MethodSource("configs")
    @ParameterizedTest
    public void crashType2DuringAuth(ProtocolVersion protocolVersion) throws Exception {
        initTestAuth(protocolVersion);
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                CrashType2After3BookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        crashType2bookieInstance = startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId);
            fail("Shouldn't get this far");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // bookie won't respond, request will timeout, and then
            // we wont be able to find a replacement
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals(0, entryCount(ledgerId.get(), bookieConf, clientConf), "Shouldn't have entry");
    }

    /**
     * Client will try to perform authentication but bookies are not configured.
     */
    @MethodSource("configs")
    @ParameterizedTest
    public void clientWithAuthAndBookieWithDisabledAuth(ProtocolVersion protocolVersion) throws Exception {
        initTestAuth(protocolVersion);
        ServerConfiguration bookieConf = newServerConfiguration();
        assertNull(bookieConf.getBookieAuthProviderFactoryClass());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        connectAndWriteToBookie(clientConf, ledgerId); // should succeed

        assertFalse(ledgerId.get() == -1);
        assertEquals(1, entryCount(ledgerId.get(), bookieConf, clientConf), "Should have entry");
    }

    /**
     * The plugin will drop the connection from the bookie side.
     */
    @MethodSource("configs")
    @ParameterizedTest
    public void dropConnectionFromBookieAuthPlugin(ProtocolVersion protocolVersion) throws Exception {
        initTestAuth(protocolVersion);
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                DropConnectionBookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId); // should fail
            fail();
        } catch (BKNotEnoughBookiesException error){
        }
    }

    BookieServer startAndStoreBookie(ServerConfiguration conf) throws Exception {
        return startAndAddBookie(conf).getServer();
    }

    /**
     * Factory for a bookie that always succeeds.
     */
    public static class AlwaysSucceedBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    addr.setAuthorizedId(new BookKeeperPrincipal("test-principal"));
                    cb.operationComplete(BKException.Code.OK, AuthToken.wrap(SUCCESS_RESPONSE));
                    completeCb.operationComplete(BKException.Code.OK, null);
                }
            };
        }
    }

    private static class LogCloseCallsBookieAuthProviderFactory implements BookieAuthProvider.Factory {

        private static AtomicInteger closeCountersOnFactory = new AtomicInteger();
        private static AtomicInteger closeCountersOnConnections = new AtomicInteger();
        private static AtomicInteger initCountersOnFactory = new AtomicInteger();
        private static AtomicInteger initCountersOnConnections = new AtomicInteger();

        @Override
        public void init(ServerConfiguration conf) throws IOException {
            initCountersOnFactory.incrementAndGet();
        }

        @Override
        public void close() {
            closeCountersOnFactory.incrementAndGet();
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer connection,
                AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                {
                    completeCb.operationComplete(BKException.Code.OK, null);
                    initCountersOnConnections.incrementAndGet();
                }

                @Override
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                }

                @Override
                public void close() {
                    closeCountersOnConnections.incrementAndGet();
                }
            };
        }

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

    }

    /**
     * Factory for a bookie that drops connections.
     */
    public static class DropConnectionBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    addr.disconnect();
                }
            };
        }
    }

    /**
     * Factory for a bookie that always fails.
     */
    public static class AlwaysFailBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    addr.setAuthorizedId(new BookKeeperPrincipal("test-principal"));
                    cb.operationComplete(BKException.Code.OK, AuthToken.wrap(FAILURE_RESPONSE));
                    completeCb.operationComplete(
                            BKException.Code.UnauthorizedAccessException, null);
                }
            };
        }
    }

    private static class LogCloseCallsClientAuthProviderFactory implements ClientAuthProvider.Factory {

        private static AtomicInteger initCountersOnFactory = new AtomicInteger();
        private static AtomicInteger initCountersOnConnections = new AtomicInteger();
        private static AtomicInteger closeCountersOnFactory = new AtomicInteger();
        private static AtomicInteger closeCountersOnConnections = new AtomicInteger();

        @Override
        public void init(ClientConfiguration conf) throws IOException {
            initCountersOnFactory.incrementAndGet();
        }

        @Override
        public ClientAuthProvider newProvider(ClientConnectionPeer connection,
                AuthCallbacks.GenericCallback<Void> completeCb) {
            return new ClientAuthProvider() {

                @Override
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                }

                @Override
                public void close() {
                    closeCountersOnConnections.incrementAndGet();
                }

                @Override
                public void init(AuthCallbacks.GenericCallback<AuthToken> cb) {
                    initCountersOnConnections.incrementAndGet();
                    completeCb.operationComplete(BKException.Code.OK, null);
                }
            };
        }

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void close() {
            closeCountersOnFactory.incrementAndGet();
        }

    }

    /**
     * Factory for bookie that will send until complete.
     */
    private static class SendUntilCompleteClientAuthProviderFactory
        implements ClientAuthProvider.Factory {

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ClientConfiguration conf) {
        }

        @Override
        public ClientAuthProvider newProvider(ClientConnectionPeer addr,
                final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new ClientAuthProvider() {
                public void init(AuthCallbacks.GenericCallback<AuthToken> cb) {
                    cb.operationComplete(BKException.Code.OK, AuthToken.wrap(PAYLOAD_MESSAGE));
                }
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    byte[] type = m.getData();
                    if (Arrays.equals(type, SUCCESS_RESPONSE)) {
                        addr.setAuthorizedId(new BookKeeperPrincipal("test-client-principal"));
                        completeCb.operationComplete(BKException.Code.OK, null);
                    } else if (Arrays.equals(type, FAILURE_RESPONSE)) {
                        completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
                    } else {
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(PAYLOAD_MESSAGE));
                    }
                }
            };
        }
    }

    /**
     * Factory for bookie that succeeds after three messages.
     */
    public static class SucceedAfter3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    if (numMessages.incrementAndGet() == 3) {
                        addr.setAuthorizedId(new BookKeeperPrincipal("test-principal"));
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(SUCCESS_RESPONSE));
                        completeCb.operationComplete(BKException.Code.OK, null);
                    } else {
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(PAYLOAD_MESSAGE));
                    }
                }
            };
        }
    }

    /**
     * Factory for bookie that fails after three messages.
     */
    public static class FailAfter3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    if (numMessages.incrementAndGet() == 3) {
                        addr.setAuthorizedId(new BookKeeperPrincipal("test-principal"));
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(FAILURE_RESPONSE));
                        completeCb.operationComplete(BKException.Code.UnauthorizedAccessException,
                                                     null);
                    } else {
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(PAYLOAD_MESSAGE));
                    }
                }
            };
        }
    }

    /**
     * Factory for crashing the bookie after 3 messages with an auth provider.
     */
    public static class CrashAfter3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    if (numMessages.incrementAndGet() == 3) {
                        throw new RuntimeException("Do bad things to the bookie");
                    } else {
                        addr.setAuthorizedId(new BookKeeperPrincipal("test-principal"));
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(PAYLOAD_MESSAGE));
                    }
                }
            };
        }
    }

    private static BookieServer crashType2bookieInstance = null;
    /**
     * Factory for a bookie with CrashType2 after three messages.
     */
    public static class CrashType2After3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    if (numMessages.incrementAndGet() != 3) {
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(PAYLOAD_MESSAGE));
                        return;
                    }
                    crashType2bookieInstance.suspendProcessing();
                }
            };
        }
    }

    /**
     * Factory for a DifferentAuthProviderPlugin.
     */
    public static class DifferentPluginBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return "DifferentAuthProviderPlugin";
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    cb.operationComplete(BKException.Code.OK, AuthToken.wrap(FAILURE_RESPONSE));
                    completeCb.operationComplete(BKException.Code.OK, null);
                }
            };
        }
    }

}
