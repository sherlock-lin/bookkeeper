/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class TestHardLink {

    private File tempDir;

    @BeforeEach
    void setup() throws IOException {
        // Create at least one file so that target disk will never be empty
        tempDir = IOUtils.createTempDir("TestHardLink", "test-hardlink");
    }

    @AfterEach
    void tearDown() throws IOException {
        FileUtils.deleteDirectory(tempDir);
    }

    private void verifyHardLink(File origin, File linkedOrigin) throws IOException {
        assertTrue(origin.exists());
        assertFalse(linkedOrigin.exists());

        HardLink.createHardLink(origin, linkedOrigin);

        assertTrue(origin.exists());
        assertTrue(linkedOrigin.exists());

        // when delete origin file it should be success and not exist.
        origin.delete();
        assertFalse(origin.exists());
        assertTrue(linkedOrigin.exists());
    }

    @Test
    void hardLink() throws IOException {
        String uuidSuffix = UUID.randomUUID().toString();

        // prepare file
        File origin = new File(tempDir, "originFile." + uuidSuffix);
        File linkedOrigin = new File(tempDir, "linkedOrigin." + uuidSuffix);
        origin.createNewFile();

        // disable jdk api link first
        HardLink.enableJdkLinkApi(false);
        verifyHardLink(origin, linkedOrigin);

        // prepare file
        File jdkorigin = new File(tempDir, "jdkoriginFile." + uuidSuffix);
        File jdklinkedOrigin = new File(tempDir, "jdklinkedOrigin." + uuidSuffix);
        jdkorigin.createNewFile();

        // enable jdk api link
        HardLink.enableJdkLinkApi(true);
        verifyHardLink(jdkorigin, jdklinkedOrigin);
    }
}
