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
package org.apache.bookkeeper.versioning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.bookkeeper.versioning.Version.Occurred;
import org.junit.jupiter.api.Test;

/**
 * Test long version.
 */
class TestLongVersion {

    @Test
    void nullIntVersion() {
        LongVersion longVersion = new LongVersion(99);
        try {
            longVersion.compare(null);
            fail("Should fail comparing with null version.");
        } catch (NullPointerException npe) {
        }
    }

    @Test
    void invalidVersion() {
        LongVersion longVersion = new LongVersion(99);
        try {
            longVersion.compare(v -> Occurred.AFTER);
            fail("Should not reach here!");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    void compare() {
        LongVersion iv = new LongVersion(99);
        assertEquals(Occurred.AFTER, iv.compare(new LongVersion(98)));
        assertEquals(Occurred.BEFORE, iv.compare(new LongVersion(100)));
        assertEquals(Occurred.CONCURRENTLY, iv.compare(new LongVersion(99)));
        assertEquals(Occurred.CONCURRENTLY, iv.compare(Version.ANY));
        assertEquals(Occurred.AFTER, iv.compare(Version.NEW));
    }
}
