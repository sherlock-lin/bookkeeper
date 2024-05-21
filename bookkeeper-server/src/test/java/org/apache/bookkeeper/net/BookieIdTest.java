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
package org.apache.bookkeeper.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for BookieId class.
 */
class BookieIdTest {

    @Test
    void testToString() {
        assertEquals("test", BookieId.parse("test").toString());
    }

    @Test
    void parse() {
        assertEquals("test", BookieId.parse("test").getId());
    }

    @Test
    void equals() {
        assertEquals(BookieId.parse("test"), BookieId.parse("test"));
        assertNotEquals(BookieId.parse("test"), BookieId.parse("test2"));
    }

    @Test
    void hashcode() {
        assertEquals(BookieId.parse("test").hashCode(), BookieId.parse("test").hashCode());
    }

    @Test
    void validate1() {
        assertThrows(IllegalArgumentException.class, () -> {
            BookieId.parse("non valid");
        });
    }

    @Test
    void validate2() {
        assertThrows(IllegalArgumentException.class, () -> {
            BookieId.parse("non$valid");
        });
    }

    @Test
    void validateReservedWord() {
        assertThrows(IllegalArgumentException.class, () -> {
            // 'readonly' is a reserved word for the ZK based implementation
            BookieId.parse("readonly");
        });
    }

    @Test
    void validateHostnamePort() {
        BookieId.parse("this.is.an.hostname:1234");
    }

    @Test
    void validateIPv4Port() {
        BookieId.parse("1.2.3.4:1234");
    }

    @Test
    void validateUUID() {
        BookieId.parse(UUID.randomUUID().toString());
    }

    @Test
    void withDashAndUnderscore() {
        BookieId.parse("testRegisterUnregister_ReadonlyBookie-readonly:3181");
    }

}
