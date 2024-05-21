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
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.client.api.BKException.Code.WriteException;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Test ensemble change has a max num.
 */
class TestMaxEnsembleChangeNum extends MockBookKeeperTestCase {

    private static final byte[] password = new byte[5];
    private static final byte[] data = new byte[5];

    @Test
    void changeEnsembleMaxNumWithWriter() throws Exception {
        long lId;
        int numEntries = 5;
        int changeNum = 5;
        setBookKeeperConfig(new ClientConfiguration().setDelayEnsembleChange(false).setMaxAllowedEnsembleChanges(5));
        try (WriteHandle writer = result(newCreateLedgerOp()
                .withAckQuorumSize(3)
                .withWriteQuorumSize(3)
                .withEnsembleSize(3)
                .withPassword(password)
                .execute())) {
            lId = writer.getId();
            //first fragment
            for (int i = 0; i < numEntries; i++) {
                writer.append(ByteBuffer.wrap(data));
            }
            assertEquals(1, getLedgerMetadata(lId).getAllEnsembles().size(), "There should be zero ensemble change");

            simulateEnsembleChangeWithWriter(changeNum, numEntries, writer);

            // one more ensemble change
            startNewBookie();
            killBookie(writer.getLedgerMetadata().getEnsembleAt(writer.getLastAddConfirmed()).get(0));
            // add failure
            try {
                writer.append(ByteBuffer.wrap(data));
                fail("should not come to here");
            } catch (BKException exception){
                assertEquals(WriteException, exception.getCode());
            }
        }
    }

    private void simulateEnsembleChangeWithWriter(int changeNum, int numEntries, WriteHandle writer) throws Exception{

        int expectedSize = writer.getLedgerMetadata().getAllEnsembles().size() + 1;
        //kill bookie and add again
        for (int num = 0; num < changeNum; num++){
            startNewBookie();

            killBookie(writer.getLedgerMetadata().getEnsembleAt(writer.getLastAddConfirmed()).get(0));
            for (int i = 0; i < numEntries; i++) {
                writer.append(ByteBuffer.wrap(data));
            }
            // ensure there is a ensemble changed
            assertEquals(expectedSize + num, writer.getLedgerMetadata().getAllEnsembles().size(), "There should be one ensemble change");
        }
    }
}
