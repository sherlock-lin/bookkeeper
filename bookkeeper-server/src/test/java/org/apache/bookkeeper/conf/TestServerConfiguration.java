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

package org.apache.bookkeeper.conf;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link ServerConfiguration}.
 */
public class TestServerConfiguration {

    private final ServerConfiguration serverConf;

    public TestServerConfiguration() {
        serverConf = new ServerConfiguration();
    }

    @BeforeEach
    void setup() throws Exception {
        serverConf.loadConf(
            getClass().getClassLoader().getResource("bk_server.conf"));
    }

    @Test
    void ephemeralPortsAllowed() throws ConfigurationException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAllowEphemeralPorts(true);
        conf.setBookiePort(0);

        conf.validate();
        assertTrue(true);
    }

    @Test
    void ephemeralPortsDisallowed() throws ConfigurationException {
        assertThrows(ConfigurationException.class, () -> {
            ServerConfiguration conf = new ServerConfiguration();
            conf.setAllowEphemeralPorts(false);
            conf.setBookiePort(0);
            conf.validate();
        });
    }

    @Test
    void setExtraServerComponents() {
        ServerConfiguration conf = new ServerConfiguration();
        assertNull(conf.getExtraServerComponents());
        String[] components = new String[] {
            "test1", "test2", "test3"
        };
        conf.setExtraServerComponents(components);
        assertArrayEquals(components, conf.getExtraServerComponents());
    }

    @Test
    void getExtraServerComponents() {
        String[] components = new String[] {
            "test1", "test2", "test3"
        };
        assertArrayEquals(components, serverConf.getExtraServerComponents());
    }

    @Test
    void mismatchofJournalAndFileInfoVersionsOlderJournalVersion() throws ConfigurationException {
        assertThrows(ConfigurationException.class, () -> {
            ServerConfiguration conf = new ServerConfiguration();
            conf.setJournalFormatVersionToWrite(5);
            conf.setFileInfoFormatVersionToWrite(1);
            conf.validate();
        });
    }

    @Test
    void mismatchofJournalAndFileInfoVersionsOlderFileInfoVersion() throws ConfigurationException {
        assertThrows(ConfigurationException.class, () -> {
            ServerConfiguration conf = new ServerConfiguration();
            conf.setJournalFormatVersionToWrite(6);
            conf.setFileInfoFormatVersionToWrite(0);
            conf.validate();
        });
    }

    @Test
    void validityOfJournalAndFileInfoVersions() throws ConfigurationException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setJournalFormatVersionToWrite(5);
        conf.setFileInfoFormatVersionToWrite(0);
        conf.validate();

        conf = new ServerConfiguration();
        conf.setJournalFormatVersionToWrite(6);
        conf.setFileInfoFormatVersionToWrite(1);
        conf.validate();
    }

    @Test
    void entryLogSizeLimit() throws ConfigurationException {
        ServerConfiguration conf = new ServerConfiguration();
        try {
            conf.setEntryLogSizeLimit(-1);
            fail("should fail setEntryLogSizeLimit since `logSizeLimit` is too small");
        } catch (IllegalArgumentException iae) {
            // expected
        }
        try {
            conf.setProperty("logSizeLimit", "-1");
            conf.validate();
            fail("Invalid configuration since `logSizeLimit` is too small");
        } catch (ConfigurationException ce) {
            // expected
        }

        try {
            conf.setEntryLogSizeLimit(2 * 1024 * 1024 * 1024L - 1);
            fail("Should fail setEntryLogSizeLimit size `logSizeLimit` is too large");
        } catch (IllegalArgumentException iae) {
            // expected
        }
        try {
            conf.validate();
            fail("Invalid configuration since `logSizeLimit` is too large");
        } catch (ConfigurationException ce) {
            // expected
        }

        conf.setEntryLogSizeLimit(512 * 1024 * 1024);
        conf.validate();
        assertEquals(512 * 1024 * 1024, conf.getEntryLogSizeLimit());

        conf.setEntryLogSizeLimit(1073741824);
        conf.validate();
        assertEquals(1073741824, conf.getEntryLogSizeLimit());
    }

    @Test
    void compactionSettings() throws ConfigurationException {
        ServerConfiguration conf = new ServerConfiguration();
        long major, minor;

        // Default Values
        major = conf.getMajorCompactionMaxTimeMillis();
        minor = conf.getMinorCompactionMaxTimeMillis();
        assertEquals(-1, major);
        assertEquals(-1, minor);

        // Set values major then minor
        conf.setMajorCompactionMaxTimeMillis(500).setMinorCompactionMaxTimeMillis(250);
        major = conf.getMajorCompactionMaxTimeMillis();
        minor = conf.getMinorCompactionMaxTimeMillis();
        assertEquals(500, major);
        assertEquals(250, minor);

        // Set values minor then major
        conf.setMinorCompactionMaxTimeMillis(150).setMajorCompactionMaxTimeMillis(1500);
        major = conf.getMajorCompactionMaxTimeMillis();
        minor = conf.getMinorCompactionMaxTimeMillis();
        assertEquals(1500, major);
        assertEquals(150, minor);

        // Default Values
        major = conf.getMajorCompactionInterval();
        minor = conf.getMinorCompactionInterval();
        assertEquals(3600, minor);
        assertEquals(86400, major);

        // Set values major then minor
        conf.setMajorCompactionInterval(43200).setMinorCompactionInterval(1800);
        major = conf.getMajorCompactionInterval();
        minor = conf.getMinorCompactionInterval();
        assertEquals(1800, minor);
        assertEquals(43200, major);

        // Set values minor then major
        conf.setMinorCompactionInterval(900).setMajorCompactionInterval(21700);
        major = conf.getMajorCompactionInterval();
        minor = conf.getMinorCompactionInterval();
        assertEquals(900, minor);
        assertEquals(21700, major);

        conf.setMinorCompactionInterval(500);
        try {
            conf.validate();
            fail();
        } catch (ConfigurationException ignore) {
        }

        conf.setMinorCompactionInterval(600);
        conf.validate();

        conf.setMajorCompactionInterval(550);
        try {
            conf.validate();
            fail();
        } catch (ConfigurationException ignore) {
        }

        conf.setMajorCompactionInterval(600);
        conf.validate();

        // Default Values
        double majorThreshold, minorThreshold;
        majorThreshold = conf.getMajorCompactionThreshold();
        minorThreshold = conf.getMinorCompactionThreshold();
        assertEquals(0.8, majorThreshold, 0.00001);
        assertEquals(0.2, minorThreshold, 0.00001);

        // Set values major then minor
        conf.setMajorCompactionThreshold(0.7).setMinorCompactionThreshold(0.1);
        majorThreshold = conf.getMajorCompactionThreshold();
        minorThreshold = conf.getMinorCompactionThreshold();
        assertEquals(0.7, majorThreshold, 0.00001);
        assertEquals(0.1, minorThreshold, 0.00001);

        // Set values minor then major
        conf.setMinorCompactionThreshold(0.3).setMajorCompactionThreshold(0.6);
        majorThreshold = conf.getMajorCompactionThreshold();
        minorThreshold = conf.getMinorCompactionThreshold();
        assertEquals(0.6, majorThreshold, 0.00001);
        assertEquals(0.3, minorThreshold, 0.00001);
    }
}
