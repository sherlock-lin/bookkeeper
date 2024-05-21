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
package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.util.BookKeeperConstants.COOKIE_NODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.server.Main;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tests 'updatecookie' shell command.
 */
public class UpdateCookieCmdTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateCookieCmdTest.class);

    MetadataBookieDriver driver;
    RegistrationManager rm;
    ServerConfiguration conf;

    public UpdateCookieCmdTest() {
        super(0);
        useUUIDasBookieId = false;
    }

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        LOG.info("setUp ZKRegistrationManager");
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        driver = MetadataDrivers.getBookieDriver(
            URI.create(baseConf.getMetadataServiceUri()));
        driver.initialize(baseConf, NullStatsLogger.INSTANCE);
        rm = driver.createRegistrationManager();

        conf = newServerConfiguration();
        LegacyCookieValidation validation = new LegacyCookieValidation(conf, rm);
        validation.checkCookies(Main.storageDirectoriesFromConf(conf));
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (rm != null) {
            rm.close();
        }
        if (driver != null) {
            driver.close();
        }
    }

    /**
     * updatecookie to hostname.
     */
    @Test
    void updateCookieIpAddressToHostname() throws Exception {
        updateCookie("-bookieId", "hostname", true);
    }

    /**
     * updatecookie to short hostname.
     */
    @Test
    void updateCookieIpAddressToShortHostname() throws Exception {
        updateCookie("-bookieId", "hostname", true, true);
    }

    /**
     * updatecookie to ipaddress.
     */
    @Test
    void updateCookieHostnameToIpAddress() throws Exception {
        updateCookie("-bookieId", "hostname", true);

        updateCookie("-b", "ip", false);

        // start bookie to ensure everything works fine
        conf.setUseHostNameAsBookieID(false);
        LegacyCookieValidation validation = new LegacyCookieValidation(conf, rm);
        validation.checkCookies(Main.storageDirectoriesFromConf(conf));
    }

    /**
     * updatecookie to invalid bookie id.
     */
    @Test
    void updateCookieWithInvalidOption() throws Exception {
        String[] argv = new String[] { "updatecookie", "-b", "invalidBookieID" };
        final ServerConfiguration conf = this.conf;
        updateCookie(argv, -1, conf);

        argv = new String[] { "updatecookie", "-b" };
        updateCookie(argv, -1, conf);

        argv = new String[] { "updatecookie" };
        updateCookie(argv, -1, conf);

        // conf not updated
        argv = new String[] { "updatecookie", "-b", "hostname" };
        conf.setUseHostNameAsBookieID(false);
        updateCookie(argv, -1, conf);

        argv = new String[] { "updatecookie", "-b", "ip" };
        conf.setUseHostNameAsBookieID(true);
        updateCookie(argv, -1, conf);
    }

    /**
     * During first updatecookie it successfully created the hostname cookie but
     * it fails to delete the old ipaddress cookie. Here user will issue
     * updatecookie again, now it should be able to delete the old cookie
     * gracefully.
     */
    @Test
    void whenBothIPaddressAndHostNameCookiesExists() throws Exception {
        updateCookie("-b", "hostname", true);

        // creates cookie with ipaddress
        final ServerConfiguration conf = this.conf;
        conf.setUseHostNameAsBookieID(true); // sets to hostname
        Cookie cookie = Cookie.readFromRegistrationManager(rm, conf).getValue();
        Cookie.Builder cookieBuilder = Cookie.newBuilder(cookie);
        conf.setUseHostNameAsBookieID(false); // sets to hostname

        final String newBookieHost = BookieImpl.getBookieAddress(conf).toString();
        cookieBuilder.setBookieId(newBookieHost);

        cookieBuilder.build().writeToRegistrationManager(rm, conf, Version.NEW);
        verifyCookieInZooKeeper(conf, 2);

        // again issue hostname cmd
        BookieShell bkShell = new BookieShell();
        conf.setUseHostNameAsBookieID(true); // sets to hostname
        bkShell.setConf(conf);
        String[] argv = new String[] { "updatecookie", "-b", "hostname" };
        assertEquals(0, bkShell.run(argv), "Failed to return the error code!");

        conf.setUseHostNameAsBookieID(true);
        cookie = Cookie.readFromRegistrationManager(rm, conf).getValue();
        assertFalse(cookie.isBookieHostCreatedFromIp(), "Cookie has created with IP!");
        // ensure the old cookie is deleted
        verifyCookieInZooKeeper(conf, 1);
    }

    /**
     * updatecookie to hostname.
     */
    @Test
    void duplicateUpdateCookieIpAddress() throws Exception {
        String[] argv = new String[] { "updatecookie", "-b", "ip" };
        final ServerConfiguration conf = this.conf;
        conf.setUseHostNameAsBookieID(true);
        updateCookie(argv, -1, conf);
    }

    @Test
    void whenNoCookieExists() throws Exception {
        String zkCookiePath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf)
            + "/" + COOKIE_NODE + "/" + BookieImpl.getBookieAddress(conf);
        assertNotNull(zkc.exists(zkCookiePath, false), "Cookie path doesn't still exists!");
        zkc.delete(zkCookiePath, -1);
        assertNull(zkc.exists(zkCookiePath, false), "Cookie path still exists!");

        BookieShell bkShell = new BookieShell();
        conf.setUseHostNameAsBookieID(true);
        bkShell.setConf(conf);
        String[] argv = new String[] { "updatecookie", "-b", "hostname" };
        assertEquals(-1, bkShell.run(argv), "Failed to return the error code!");
    }

    private void verifyCookieInZooKeeper(ServerConfiguration conf, int expectedCount) throws KeeperException,
            InterruptedException {
        List<String> cookies;
        String bookieCookiePath1 = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf) + "/" + COOKIE_NODE;
        cookies = zkc.getChildren(bookieCookiePath1, false);
        assertEquals(expectedCount, cookies.size(), "Wrongly updated the cookie!");
    }

    private void updateCookie(String option, String optionVal, boolean useHostNameAsBookieID) throws Exception {
        updateCookie(option, optionVal, useHostNameAsBookieID, false);
    }

    private void updateCookie(String option, String optionVal, boolean useHostNameAsBookieID, boolean useShortHostName)
            throws Exception {
        conf.setUseHostNameAsBookieID(!useHostNameAsBookieID);
        Cookie cookie = Cookie.readFromRegistrationManager(rm, conf).getValue();
        final boolean previousBookieID = cookie.isBookieHostCreatedFromIp();
        assertEquals(useHostNameAsBookieID, previousBookieID, "Wrong cookie!");

        LOG.info("Perform updatecookie command");
        ServerConfiguration newconf = new ServerConfiguration(conf);
        newconf.setUseHostNameAsBookieID(useHostNameAsBookieID);
        newconf.setUseShortHostName(useShortHostName);
        BookieShell bkShell = new BookieShell();
        bkShell.setConf(newconf);
        String[] argv = new String[] { "updatecookie", option, optionVal };
        assertEquals(0, bkShell.run(argv), "Failed to return exit code!");

        newconf.setUseHostNameAsBookieID(useHostNameAsBookieID);
        newconf.setUseShortHostName(useShortHostName);
        cookie = Cookie.readFromRegistrationManager(rm, newconf).getValue();
        assertEquals(previousBookieID, !cookie.isBookieHostCreatedFromIp(), "Wrongly updated cookie!");
        assertEquals(useHostNameAsBookieID, !cookie.isBookieHostCreatedFromIp(), "Wrongly updated cookie!");
        verifyCookieInZooKeeper(newconf, 1);

        for (File journalDir : conf.getJournalDirs()) {
            journalDir = BookieImpl.getCurrentDirectory(journalDir);
            Cookie jCookie = Cookie.readFromDirectory(journalDir);
            jCookie.verify(cookie);
        }
        File[] ledgerDir = BookieImpl.getCurrentDirectories(conf.getLedgerDirs());
        for (File dir : ledgerDir) {
            Cookie lCookie = Cookie.readFromDirectory(dir);
            lCookie.verify(cookie);
        }
    }

    private void updateCookie(String[] argv, int exitCode, ServerConfiguration conf) throws KeeperException,
            InterruptedException, IOException, UnknownHostException, Exception {
        LOG.info("Perform updatecookie command");
        BookieShell bkShell = new BookieShell();
        bkShell.setConf(conf);

        assertEquals(exitCode, bkShell.run(argv), "Failed to return exit code!");
    }
}
