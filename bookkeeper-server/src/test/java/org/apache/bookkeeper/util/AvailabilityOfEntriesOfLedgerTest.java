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
package org.apache.bookkeeper.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Testsuite for AvailabilityOfEntriesOfLedger.
 */
class AvailabilityOfEntriesOfLedgerTest {
    @Test
    void withItrConstructor() {
        long[][] arrays = {
                { 0, 1, 2 },
                { 1, 2},
                { 1, 2, 3, 5, 6, 7, 8 },
                { 0, 1, 5 },
                { 3 },
                { 1, 2, 4, 5, 7, 8 },
                {},
                {0},
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 100, 1000, 1001, 10000, 20000, 20001 }
        };
        for (int i = 0; i < arrays.length; i++) {
            long[] tempArray = arrays[i];
            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(tempArray).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);
            assertEquals(tempArray.length,
                    availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries(),
                    "Expected total number of entries");
            for (int j = 0; j < tempArray.length; j++) {
                assertTrue(availabilityOfEntriesOfLedger.isEntryAvailable(tempArray[j]),
                        tempArray[j] + " is supposed to be available");
            }
        }
    }

    @Test
    void withItrConstructorWithDuplicates() {
        long[][] arrays = {
                { 1, 2, 2, 3 },
                { 1, 2, 3, 5, 5, 6, 7, 8, 8 },
                { 1, 1, 5, 5 },
                { 3, 3 },
                { 1, 1, 2, 4, 5, 8, 9, 9, 9, 9, 9 },
                {},
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 17, 100, 1000, 1000, 1001, 10000, 10000, 20000, 20001 }
        };
        for (int i = 0; i < arrays.length; i++) {
            long[] tempArray = arrays[i];
            Set<Long> tempSet = new HashSet<Long>();
            for (int k = 0; k < tempArray.length; k++) {
                tempSet.add(tempArray[k]);
            }
            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(tempArray).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);
            assertEquals(tempSet.size(),
                    availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries(),
                    "Expected total number of entries");
            for (int j = 0; j < tempArray.length; j++) {
                assertTrue(availabilityOfEntriesOfLedger.isEntryAvailable(tempArray[j]),
                        tempArray[j] + " is supposed to be available");
            }
        }
    }

    @Test
    void serializeDeserialize() {
        long[][] arrays = {
                { 0, 1, 2 },
                { 1, 2 },
                { 1, 2, 3, 5, 6, 7, 8 },
                { 0, 1, 5 },
                { 3 },
                { 1, 2, 4, 5, 7, 8 },
                { },
                { 0 },
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 100, 1000, 1001, 10000, 20000, 20001 }
        };
        for (int i = 0; i < arrays.length; i++) {
            long[] tempArray = arrays[i];
            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(tempArray).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);
            byte[] serializedState = availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedgerUsingSer = new AvailabilityOfEntriesOfLedger(
                    serializedState);
            assertEquals(tempArray.length,
                    availabilityOfEntriesOfLedgerUsingSer.getTotalNumOfAvailableEntries(),
                    "Expected total number of entries");
            for (int j = 0; j < tempArray.length; j++) {
                assertTrue(availabilityOfEntriesOfLedgerUsingSer.isEntryAvailable(tempArray[j]),
                        tempArray[j] + " is supposed to be available");
            }
        }
    }

    @Test
    void serializeDeserializeWithItrConstructorWithDuplicates() {
        long[][] arrays = {
                { 1, 2, 2, 3 },
                { 1, 2, 3, 5, 5, 6, 7, 8, 8 },
                { 1, 1, 5, 5 },
                { 3, 3 },
                { 1, 1, 2, 4, 5, 8, 9, 9, 9, 9, 9 },
                {},
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 17, 100, 1000, 1000, 1001, 10000, 10000, 20000, 20001 }
        };
        for (int i = 0; i < arrays.length; i++) {
            long[] tempArray = arrays[i];
            Set<Long> tempSet = new HashSet<Long>();
            for (int k = 0; k < tempArray.length; k++) {
                tempSet.add(tempArray[k]);
            }
            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(tempArray).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);
            byte[] serializedState = availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedgerUsingSer = new AvailabilityOfEntriesOfLedger(
                    serializedState);
            assertEquals(tempSet.size(),
                    availabilityOfEntriesOfLedgerUsingSer.getTotalNumOfAvailableEntries(),
                    "Expected total number of entries");
            for (int j = 0; j < tempArray.length; j++) {
                assertTrue(availabilityOfEntriesOfLedgerUsingSer.isEntryAvailable(tempArray[j]),
                        tempArray[j] + " is supposed to be available");
            }
        }
    }

    @Test
    void nonExistingEntries() {
        long[][] arrays = {
                { 0, 1, 2 },
                { 1, 2, 3, 5, 6, 7, 8 },
                { 1, 5 },
                { 3 },
                { 1, 2, 4, 5, 7, 8 },
                {},
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 100, 1000, 1001, 10000, 20000, 20001 }
        };
        /**
         * corresponding non-existing entries for 'arrays'
         */
        long[][] nonExistingEntries = {
                { 3 },
                { 0, 4, 9, 100, 101 },
                { 0, 2, 3, 6, 9 },
                { 0, 1, 2, 4, 5, 6 },
                { 0, 3, 9, 10, 11, 100, 1000 },
                { 0, 1, 2, 3, 4, 5 },
                { 4, 18, 1002, 19999, 20003 }
        };
        for (int i = 0; i < arrays.length; i++) {
            long[] tempArray = arrays[i];
            long[] nonExistingElementsTempArray = nonExistingEntries[i];
            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(tempArray).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);

            for (int j = 0; j < nonExistingElementsTempArray.length; j++) {
                assertFalse(availabilityOfEntriesOfLedger.isEntryAvailable(nonExistingElementsTempArray[j]),
                        nonExistingElementsTempArray[j] + " is not supposed to be available");
            }
        }
    }

    @Test
    void getUnavailableEntries() {
        /*
         * AvailabilityOfEntriesOfLedger is going to be created with this
         * entries. It is equivalent to considering that Bookie has these
         * entries.
         */
        long[][] availableEntries = {
                { 1, 2},
                { 0, 1, 2 },
                { 1, 2, 3, 5, 6, 7, 8 },
                { 1, 5 },
                { 3 },
                { 1, 2, 4, 5, 7, 8 },
                {},
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 100, 1000, 1001, 10000, 20000, 20001 }
        };

        /*
         * getUnavailableEntries method is going to be called with these entries
         * as expected to contain.
         */
        long[][] expectedToContainEntries = {
                { 1, 2},
                { 0, 1, 2, 3, 5 },
                { 1, 2, 5, 7, 8 },
                { 2, 7 },
                { 3 },
                { 1, 5, 7, 8, 9, 10 },
                { 0, 1, 2, 3, 4, 5 },
                { 4, 18, 1002, 19999, 20003 }
        };

        /*
         * Considering what AvailabilityOfEntriesOfLedger contains
         * (availableEntries), what it is expected to contain
         * (expectedToContainEntries), following are the entries which are
         * supposed to be reported as unavailable (unavailableEntries).
         */
        long[][] unavailableEntries = {
                { },
                { 3, 5 },
                { },
                { 2, 7 },
                { },
                { 9, 10 },
                { 0, 1, 2, 3, 4, 5 },
                { 4, 18, 1002, 19999, 20003 }
        };

        for (int i = 0; i < availableEntries.length; i++) {
            long[] availableEntriesTempArray = availableEntries[i];
            long[] expectedToContainEntriesTempArray = expectedToContainEntries[i];
            long[] unavailableEntriesTempArray = unavailableEntries[i];
            List<Long> unavailableEntriesTempList = new ArrayList<Long>();
            for (int j = 0; j < unavailableEntriesTempArray.length; j++) {
                unavailableEntriesTempList.add(unavailableEntriesTempArray[j]);
            }

            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(availableEntriesTempArray).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);

            long startEntryId;
            long lastEntryId;
            if (expectedToContainEntriesTempArray[0] == 0) {
                startEntryId = expectedToContainEntriesTempArray[0];
                lastEntryId = expectedToContainEntriesTempArray[expectedToContainEntriesTempArray.length - 1];
            } else {
                startEntryId = expectedToContainEntriesTempArray[0] - 1;
                lastEntryId = expectedToContainEntriesTempArray[expectedToContainEntriesTempArray.length - 1] + 1;
            }
            BitSet expectedToContainEntriesBitSet = new BitSet((int) (lastEntryId - startEntryId + 1));
            for (int ind = 0; ind < expectedToContainEntriesTempArray.length; ind++) {
                int entryId = (int) expectedToContainEntriesTempArray[ind];
                expectedToContainEntriesBitSet.set(entryId - (int) startEntryId);
            }

            List<Long> actualUnavailableEntries = availabilityOfEntriesOfLedger.getUnavailableEntries(startEntryId,
                    lastEntryId, expectedToContainEntriesBitSet);
            assertEquals(unavailableEntriesTempList, actualUnavailableEntries, "Unavailable Entries");
        }
    }

    @Test
    void emptyAvailabilityOfEntriesOfLedger() {
        AvailabilityOfEntriesOfLedger emptyOne = AvailabilityOfEntriesOfLedger.EMPTY_AVAILABILITYOFENTRIESOFLEDGER;
        assertEquals(0, emptyOne.getTotalNumOfAvailableEntries(), "expected totalNumOfAvailableEntries");
        assertFalse(emptyOne.isEntryAvailable(100L), "empty one is not supposed to contain any entry");
        long startEntryId = 100;
        long lastEntryId = 105;
        BitSet bitSetOfAvailability = new BitSet((int) (lastEntryId - startEntryId + 1));
        for (int i = 0; i < bitSetOfAvailability.length(); i++) {
            if ((i % 2) == 0) {
                bitSetOfAvailability.set(i);
            }
        }
        List<Long> unavailableEntries = emptyOne.getUnavailableEntries(startEntryId, lastEntryId, bitSetOfAvailability);
        assertEquals(bitSetOfAvailability.cardinality(), unavailableEntries.size(), "Num of unavailable entries");
        for (int i = 0; i < bitSetOfAvailability.length(); i++) {
            long entryId = startEntryId + i;
            if (bitSetOfAvailability.get(i)) {
                assertTrue(unavailableEntries.contains(entryId), "Unavailable entry");
            }
        }
    }
}
