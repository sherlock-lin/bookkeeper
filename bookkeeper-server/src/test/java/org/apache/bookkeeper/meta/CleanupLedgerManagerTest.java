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

package org.apache.bookkeeper.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test of {@link CleanupLedgerManager}.
 */
class CleanupLedgerManagerTest {

    protected LedgerManager ledgerManager = null;
    protected CleanupLedgerManager cleanupLedgerManager = null;

    @BeforeEach
    void setup() throws Exception {
        ledgerManager = mock(LedgerManager.class);
        CompletableFuture<Versioned<LedgerMetadata>> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("LedgerNotExistException"));
        when(ledgerManager.createLedgerMetadata(anyLong(), any())).thenReturn(future);
        when(ledgerManager.readLedgerMetadata(anyLong())).thenReturn(future);
        when(ledgerManager.writeLedgerMetadata(anyLong(), any(), any())).thenReturn(
                future);
        CompletableFuture<Void> removeFuture = new CompletableFuture<>();
        removeFuture.completeExceptionally(new Exception("LedgerNotExistException"));
        when(ledgerManager.removeLedgerMetadata(anyLong(), any())).thenReturn(removeFuture);
        cleanupLedgerManager = new CleanupLedgerManager(ledgerManager);
    }

    @Test
    void createLedgerMetadataException() throws Exception {
        cleanupLedgerManager.createLedgerMetadata(anyLong(), any(LedgerMetadata.class));
        assertEquals(0, cleanupLedgerManager.getCurrentFuturePromiseSize());
    }

    @Test
    void readLedgerMetadataException() throws Exception {
        cleanupLedgerManager.readLedgerMetadata(anyLong());
        assertEquals(0, cleanupLedgerManager.getCurrentFuturePromiseSize());
    }

    @Test
    void writeLedgerMetadataException() throws Exception {
        cleanupLedgerManager.writeLedgerMetadata(anyLong(), any(LedgerMetadata.class), any(Version.class));
        assertEquals(0, cleanupLedgerManager.getCurrentFuturePromiseSize());
    }

    @Test
    void removeLedgerMetadataException() throws Exception {
        cleanupLedgerManager.removeLedgerMetadata(anyLong(), any(Version.class));
        assertEquals(0, cleanupLedgerManager.getCurrentFuturePromiseSize());
    }
}
