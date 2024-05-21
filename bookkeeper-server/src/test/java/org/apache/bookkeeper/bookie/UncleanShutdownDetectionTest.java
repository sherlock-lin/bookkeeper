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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test the unclean shutdown implementation.
 */
public class UncleanShutdownDetectionTest {

    @TempDir
    public File tempDir;

    @Test
    void registerStartWithoutRegisterShutdownEqualsUncleanShutdown() throws IOException {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(
                conf, conf.getLedgerDirs(), diskChecker);

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
        uncleanShutdownDetection.registerStartUp();

        assertTrue(uncleanShutdownDetection.lastShutdownWasUnclean());
    }

    @Test
    void registerStartWithRegisterShutdownEqualsCleanShutdown() throws IOException {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(
                conf, conf.getLedgerDirs(), diskChecker);

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
        uncleanShutdownDetection.registerStartUp();
        uncleanShutdownDetection.registerCleanShutdown();

        assertFalse(uncleanShutdownDetection.lastShutdownWasUnclean());
    }

    @Test
    void registerStartWithoutRegisterShutdownEqualsUncleanShutdownMultipleDirs() throws IOException {
        File ledgerDir1 = newFolder(tempDir, "l1");
        File ledgerDir2 = newFolder(tempDir, "l2");
        File ledgerDir3 = newFolder(tempDir, "l3");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setLedgerDirNames(new String[] {ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath(),
                        ledgerDir3.getAbsolutePath()});
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(
                conf, conf.getLedgerDirs(), diskChecker);

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
        uncleanShutdownDetection.registerStartUp();

        assertTrue(uncleanShutdownDetection.lastShutdownWasUnclean());
    }

    @Test
    void registerStartWithRegisterShutdownEqualsCleanShutdownMultipleDirs() throws IOException {
        File ledgerDir1 = newFolder(tempDir, "l1");
        File ledgerDir2 = newFolder(tempDir, "l2");
        File ledgerDir3 = newFolder(tempDir, "l3");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setLedgerDirNames(new String[] {ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath(),
                        ledgerDir3.getAbsolutePath()});
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(
                conf, conf.getLedgerDirs(), diskChecker);

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
        uncleanShutdownDetection.registerStartUp();
        uncleanShutdownDetection.registerCleanShutdown();

        assertFalse(uncleanShutdownDetection.lastShutdownWasUnclean());
    }

    @Test
    void registerStartWithPartialRegisterShutdownEqualsUncleanShutdownMultipleDirs() throws IOException {
        File ledgerDir1 = newFolder(tempDir, "l1");
        File ledgerDir2 = newFolder(tempDir, "l2");
        File ledgerDir3 = newFolder(tempDir, "l3");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setLedgerDirNames(new String[] {ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath(),
                        ledgerDir3.getAbsolutePath()});
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(
                conf, conf.getLedgerDirs(), diskChecker);

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
        uncleanShutdownDetection.registerStartUp();
        uncleanShutdownDetection.registerCleanShutdown();
        File dirtyFile = new File(ledgerDirsManager.getAllLedgerDirs().get(0),
                UncleanShutdownDetectionImpl.DIRTY_FILENAME);
        dirtyFile.createNewFile();

        assertTrue(uncleanShutdownDetection.lastShutdownWasUnclean());
    }

    @Test
    void registerStartFailsToCreateDirtyFilesAndThrowsIOException() throws IOException {
        assertThrows(IOException.class, () -> {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
            LedgerDirsManager ledgerDirsManager = new MockLedgerDirsManager(conf, conf.getLedgerDirs(), diskChecker);

            UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
            uncleanShutdownDetection.registerStartUp();
        });
    }

    private class MockLedgerDirsManager extends LedgerDirsManager {
        public MockLedgerDirsManager(ServerConfiguration conf, File[] dirs, DiskChecker diskChecker)
                throws IOException {
            super(conf, dirs, diskChecker);
        }

        @Override
        public List<File> getAllLedgerDirs() {
            List<File> dirs = new ArrayList<>();
            dirs.add(new File("does_not_exist"));
            return dirs;
        }

        private static File newFolder(File root, String... subDirs) throws IOException {
            String subFolder = String.join("/", subDirs);
            File result = new File(root, subFolder);
            if (!result.mkdirs()) {
                throw new IOException("Couldn't create folders " + root);
            }
            return result;
        }
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
        String subFolder = String.join("/", subDirs);
        File result = new File(root, subFolder);
        if (!result.mkdirs()) {
            throw new IOException("Couldn't create folders " + root);
        }
        return result;
    }
}
