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
package org.apache.bookkeeper.client.api;

import static org.apache.bookkeeper.client.api.WriteFlag.DEFERRED_SYNC;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKClientClosedException;
import org.apache.bookkeeper.client.BKException.BKIncorrectParameterException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.MockBookKeeperTestCase;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Unit tests of builders.
 */
class BookKeeperBuildersTest extends MockBookKeeperTestCase {

    private static final int ensembleSize = 3;
    private static final int writeQuorumSize = 2;
    private static final int ackQuorumSize = 1;
    private static final long ledgerId = 12342L;
    private static final Map<String, byte[]> customMetadata = new HashMap<>();
    private static final byte[] password = new byte[3];
    private static final byte[] entryData = new byte[32];
    private static final EnumSet<WriteFlag> writeFlagsDeferredSync = EnumSet.of(DEFERRED_SYNC);

    @Test
    void createLedger() throws Exception {
        setNewGeneratedLedgerId(ledgerId);
        WriteHandle writer = newCreateLedgerOp()
            .withAckQuorumSize(ackQuorumSize)
            .withEnsembleSize(ensembleSize)
            .withWriteQuorumSize(writeQuorumSize)
            .withCustomMetadata(customMetadata)
            .withPassword(password)
            .execute()
            .get();
        assertEquals(ledgerId, writer.getId());
        LedgerMetadata metadata = getLedgerMetadata(ledgerId);
        assertEquals(ensembleSize, metadata.getEnsembleSize());
        assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
        assertEquals(writeQuorumSize, metadata.getWriteQuorumSize());
        assertArrayEquals(password, metadata.getPassword());
    }

    @Test
    void failEnsembleSize0() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newCreateLedgerOp()
                    .withEnsembleSize(0)
                    .withPassword(password)
                    .execute());
        });
    }

    @Test
    void failWriteQuorumSize0() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newCreateLedgerOp()
                    .withEnsembleSize(2)
                    .withWriteQuorumSize(0)
                    .withPassword(password)
                    .execute());
        });
    }

    @Test
    void failNullWriteFlags() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newCreateLedgerOp()
                    .withWriteFlags((EnumSet<WriteFlag>) null)
                    .withPassword(password)
                    .execute());
        });
    }

    @Test
    void failAckQuorumSize0() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newCreateLedgerOp()
                    .withEnsembleSize(2)
                    .withWriteQuorumSize(1)
                    .withAckQuorumSize(0)
                    .withPassword(password)
                    .execute());
        });
    }

    @Test
    void failWriteQuorumSizeGreaterThanEnsembleSize() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newCreateLedgerOp()
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(2)
                    .withAckQuorumSize(1)
                    .withPassword(password)
                    .execute());
        });
    }

    @Test
    void failAckQuorumSizeGreaterThanWriteQuorumSize() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newCreateLedgerOp()
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withAckQuorumSize(2)
                    .withPassword(password)
                    .execute());
        });
    }

    @Test
    void failNoPassword() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newCreateLedgerOp()
                    .execute());
        });
    }

    @Test
    void failPasswordNull() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newCreateLedgerOp()
                    .withPassword(null)
                    .execute());
        });
    }

    @Test
    void failCustomMetadataNull() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newCreateLedgerOp()
                    .withCustomMetadata(null)
                    .withPassword(password)
                    .execute());
        });
    }

    @Test
    void failDigestTypeNullAndAutodetectionTrue() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            ClientConfiguration config = new ClientConfiguration();
            config.setEnableDigestTypeAutodetection(true);
            setBookKeeperConfig(config);
            result(newCreateLedgerOp()
                    .withDigestType(null)
                    .withPassword(password)
                    .execute());
        });
    }

    @Test
    void failDigestTypeNullAndAutodetectionFalse() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            ClientConfiguration config = new ClientConfiguration();
            config.setEnableDigestTypeAutodetection(false);
            setBookKeeperConfig(config);
            result(newCreateLedgerOp()
                    .withDigestType(null)
                    .withPassword(password)
                    .execute());
            fail("shoud not be able to create a ledger with such specs");
        });
    }

    @Test
    void failDigestTypeNullAndBookkKeeperClosed() throws Exception {
        assertThrows(BKClientClosedException.class, () -> {
            closeBookkeeper();
            result(newCreateLedgerOp()
                    .withPassword(password)
                    .execute());
            fail("shoud not be able to create a ledger, client is closed");
        });
    }

    @Test
    void createAdvLedger() throws Exception {
        setNewGeneratedLedgerId(ledgerId);
        WriteAdvHandle writer = newCreateLedgerOp()
            .withAckQuorumSize(ackQuorumSize)
            .withEnsembleSize(ensembleSize)
            .withPassword(password)
            .withWriteQuorumSize(writeQuorumSize)
            .withCustomMetadata(customMetadata)
            .makeAdv()
            .execute()
            .get();
        assertEquals(ledgerId, writer.getId());
        LedgerMetadata metadata = getLedgerMetadata(ledgerId);
        assertEquals(ensembleSize, metadata.getEnsembleSize());
        assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
        assertEquals(writeQuorumSize, metadata.getWriteQuorumSize());
        assertArrayEquals(password, metadata.getPassword());
    }

    @Test
    void defaultWriteFlagsEmpty() throws Exception {
        setNewGeneratedLedgerId(ledgerId);
        WriteHandle writer = newCreateLedgerOp()
            .withAckQuorumSize(ackQuorumSize)
            .withEnsembleSize(ensembleSize)
            .withPassword(password)
            .withWriteQuorumSize(writeQuorumSize)
            .withCustomMetadata(customMetadata)
            .execute()
            .get();
        assertEquals(ledgerId, writer.getId());
        LedgerMetadata metadata = getLedgerMetadata(ledgerId);
        assertEquals(ensembleSize, metadata.getEnsembleSize());
        assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
        assertEquals(writeQuorumSize, metadata.getWriteQuorumSize());
        assertArrayEquals(password, metadata.getPassword());
        LedgerHandle lh = (LedgerHandle) writer;
        assertEquals(WriteFlag.NONE, lh.getWriteFlags());
    }

    @Test
    void createAdvLedgerWriteFlags() throws Exception {
        setNewGeneratedLedgerId(ledgerId);
        WriteAdvHandle writer = newCreateLedgerOp()
            .withWriteFlags(writeFlagsDeferredSync)
            .withAckQuorumSize(ackQuorumSize)
            .withEnsembleSize(ensembleSize)
            .withPassword(password)
            .withWriteQuorumSize(writeQuorumSize)
            .withCustomMetadata(customMetadata)
            .makeAdv()
            .execute()
            .get();
        assertEquals(ledgerId, writer.getId());
        LedgerMetadata metadata = getLedgerMetadata(ledgerId);
        assertEquals(ensembleSize, metadata.getEnsembleSize());
        assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
        assertEquals(writeQuorumSize, metadata.getWriteQuorumSize());
        assertArrayEquals(password, metadata.getPassword());
        LedgerHandle lh = (LedgerHandle) writer;
        assertEquals(writeFlagsDeferredSync, lh.getWriteFlags());
    }

    @Test
    void createLedgerWriteFlags() throws Exception {
        setNewGeneratedLedgerId(ledgerId);
        WriteHandle writer = newCreateLedgerOp()
            .withWriteFlags(writeFlagsDeferredSync)
            .withAckQuorumSize(ackQuorumSize)
            .withEnsembleSize(ensembleSize)
            .withPassword(password)
            .withWriteQuorumSize(writeQuorumSize)
            .withCustomMetadata(customMetadata)
            .execute()
            .get();
        assertEquals(ledgerId, writer.getId());
        LedgerMetadata metadata = getLedgerMetadata(ledgerId);
        assertEquals(ensembleSize, metadata.getEnsembleSize());
        assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
        assertEquals(writeQuorumSize, metadata.getWriteQuorumSize());
        assertArrayEquals(password, metadata.getPassword());
        LedgerHandle lh = (LedgerHandle) writer;
        assertEquals(writeFlagsDeferredSync, lh.getWriteFlags());
    }

    @Test
    void createLedgerWriteFlagsVarargs() throws Exception {
        setNewGeneratedLedgerId(ledgerId);
        WriteHandle writer = newCreateLedgerOp()
            .withWriteFlags(DEFERRED_SYNC)
            .withAckQuorumSize(ackQuorumSize)
            .withEnsembleSize(ensembleSize)
            .withPassword(password)
            .withWriteQuorumSize(writeQuorumSize)
            .withCustomMetadata(customMetadata)
            .execute()
            .get();
        assertEquals(ledgerId, writer.getId());
        LedgerMetadata metadata = getLedgerMetadata(ledgerId);
        assertEquals(ensembleSize, metadata.getEnsembleSize());
        assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
        assertEquals(writeQuorumSize, metadata.getWriteQuorumSize());
        assertArrayEquals(password, metadata.getPassword());
        LedgerHandle lh = (LedgerHandle) writer;
        assertEquals(writeFlagsDeferredSync, lh.getWriteFlags());
    }

    @Test
    void failCreateAdvLedgerBadFixedLedgerIdMinus1() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newCreateLedgerOp()
                    .withPassword(password)
                    .makeAdv()
                    .withLedgerId(-1)
                    .execute());
        });
    }

    @Test
    void failCreateAdvLedgerBadFixedLedgerIdNegative() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newCreateLedgerOp()
                    .withPassword(password)
                    .makeAdv()
                    .withLedgerId(-2)
                    .execute());
            fail("shoud not be able to create a ledger with such specs");
        });
    }

    @Test
    void openLedgerNoId() throws Exception {
        assertThrows(BKNoSuchLedgerExistsOnMetadataServerException.class, () -> {
            result(newOpenLedgerOp().execute());
        });
    }

    @Test
    void openLedgerBadId() throws Exception {
        assertThrows(BKNoSuchLedgerExistsOnMetadataServerException.class, () -> {
            result(newOpenLedgerOp()
                    .withPassword(password)
                    .withLedgerId(ledgerId)
                    .execute());
        });
    }

    @Test
    void openLedgerClientClosed() throws Exception {
        assertThrows(BKClientClosedException.class, () -> {
            closeBookkeeper();
            result(newOpenLedgerOp()
                    .withPassword(password)
                    .withLedgerId(ledgerId)
                    .execute());
        });
    }

    @Test
    void deleteLedgerNoLedgerId() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newDeleteLedgerOp()
                    .execute());
        });
    }

    @Test
    void deleteLedgerBadLedgerId() throws Exception {
        assertThrows(BKIncorrectParameterException.class, () -> {
            result(newDeleteLedgerOp()
                    .withLedgerId(-1)
                    .execute());
        });
    }

    @Test
    void deleteLedger() throws Exception {
        LedgerMetadata ledgerMetadata = generateLedgerMetadata(ensembleSize,
            writeQuorumSize, ackQuorumSize, password, customMetadata);
        registerMockLedgerMetadata(ledgerId, ledgerMetadata);

        result(newDeleteLedgerOp()
            .withLedgerId(ledgerId)
            .execute());
    }

    @Test
    void deleteLedgerBookKeeperClosed() throws Exception {
        assertThrows(BKClientClosedException.class, () -> {
            closeBookkeeper();
            result(newDeleteLedgerOp()
                    .withLedgerId(ledgerId)
                    .execute());
        });
    }

    protected LedgerMetadata generateLedgerMetadata(int ensembleSize,
        int writeQuorumSize, int ackQuorumSize, byte[] password,
        Map<String, byte[]> customMetadata) throws BKException.BKNotEnoughBookiesException {
        return LedgerMetadataBuilder.create()
            .withId(12L)
            .withEnsembleSize(ensembleSize)
            .withWriteQuorumSize(writeQuorumSize)
            .withAckQuorumSize(ackQuorumSize)
            .withPassword(password)
            .withDigestType(BookKeeper.DigestType.CRC32.toApiDigestType())
            .withCustomMetadata(customMetadata)
            .withCreationTime(System.currentTimeMillis())
            .newEnsembleEntry(0, generateNewEnsemble(ensembleSize)).build();
    }

    @Test
    void createLedgerWithOpportunisticStriping() throws Exception {

        maxNumberOfAvailableBookies =  4;
        int bigEnsembleSize = 15;
        int expectedWriteQuorumSize = 4;

        ClientConfiguration config = new ClientConfiguration();
        config.setOpportunisticStriping(true);
        setBookKeeperConfig(config);

        setNewGeneratedLedgerId(ledgerId);
        WriteHandle writer = newCreateLedgerOp()
            .withAckQuorumSize(expectedWriteQuorumSize)
            .withEnsembleSize(bigEnsembleSize)
            .withWriteQuorumSize(expectedWriteQuorumSize)
            .withCustomMetadata(customMetadata)
            .withPassword(password)
            .execute()
            .get();
        assertEquals(ledgerId, writer.getId());
        LedgerMetadata metadata = getLedgerMetadata(ledgerId);
        assertEquals(expectedWriteQuorumSize, metadata.getEnsembleSize());
        assertEquals(expectedWriteQuorumSize, metadata.getAckQuorumSize());
        assertEquals(expectedWriteQuorumSize, metadata.getWriteQuorumSize());
        assertArrayEquals(password, metadata.getPassword());

    }

    @Test
    void notEnoughBookies() throws Exception {
        assertThrows(BKException.BKNotEnoughBookiesException.class, () -> {

            maxNumberOfAvailableBookies = 1;
            ClientConfiguration config = new ClientConfiguration();
            config.setOpportunisticStriping(false);
            setBookKeeperConfig(config);

            setNewGeneratedLedgerId(ledgerId);
            result(newCreateLedgerOp()
                    .withAckQuorumSize(ackQuorumSize)
                    .withEnsembleSize(ensembleSize)
                    .withWriteQuorumSize(writeQuorumSize)
                    .withCustomMetadata(customMetadata)
                    .withPassword(password)
                    .execute());
        });
    }

}
