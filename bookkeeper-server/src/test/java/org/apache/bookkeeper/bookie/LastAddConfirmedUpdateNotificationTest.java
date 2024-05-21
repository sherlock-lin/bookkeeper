/*
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
 */

package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit test of {@link LastAddConfirmedUpdateNotification}.
 */
class LastAddConfirmedUpdateNotificationTest {

    @Test
    void getters() {
        long lac = System.currentTimeMillis();
        LastAddConfirmedUpdateNotification notification = LastAddConfirmedUpdateNotification.of(lac);

        long timestamp = System.currentTimeMillis();
        assertEquals(lac, notification.getLastAddConfirmed());
        assertTrue(notification.getTimestamp() <= timestamp);

        notification.recycle();
    }

    @Test
    void recycle() {
        long lac = System.currentTimeMillis();
        LastAddConfirmedUpdateNotification notification = LastAddConfirmedUpdateNotification.of(lac);
        notification.recycle();

        assertEquals(-1L, notification.getLastAddConfirmed());
        assertEquals(-1L, notification.getTimestamp());
    }

    @Test
    void func() {
        long lac = System.currentTimeMillis();
        LastAddConfirmedUpdateNotification notification = LastAddConfirmedUpdateNotification.FUNC.apply(lac);

        assertEquals(lac, notification.getLastAddConfirmed());
    }

}
