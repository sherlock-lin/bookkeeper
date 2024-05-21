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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;

/**
 * Tests for BookieSocketAddress getSocketAddress cache logic.
 */

class ResolvedBookieSocketAddressTest {

    @Test
    void hostnameBookieId() throws Exception {
        BookieSocketAddress hostnameAddress = new BookieSocketAddress("localhost", 3181);
        InetSocketAddress inetSocketAddress1 = hostnameAddress.getSocketAddress();
        InetSocketAddress inetSocketAddress2 = hostnameAddress.getSocketAddress();
        assertFalse(inetSocketAddress1 == inetSocketAddress2, "InetSocketAddress should be recreated");
    }

    @Test
    void iPAddressBookieId() throws Exception {
        BookieSocketAddress ipAddress = new BookieSocketAddress("127.0.0.1", 3181);
        InetSocketAddress inetSocketAddress1 = ipAddress.getSocketAddress();
        InetSocketAddress inetSocketAddress2 = ipAddress.getSocketAddress();
        assertTrue(inetSocketAddress1 == inetSocketAddress2, "InetSocketAddress should be cached");
    }
}
