/**
 * Copyright The Apache Software Foundation
 *
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Testsuite for IteratorUtility methods.
 */
class IteratorUtilityTest {

    @Test
    void withPrimitiveItrMerge() {
        long[][] arrays = {
                { 0, 1, 2 },
                { 0, 1 },
                { 1, 2 },
                { 1, 2, 3, 5, 6, 7, 8 },
                { 1, 2, 3, 5, 6, 7, 8 },
                { 0, 1, 5 },
                { 3 },
                { 1, 2, 4, 5, 7, 8 },
                {},
                {},
                { 0 },
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 100, 1000, 1001, 10000, 20000, 20001 },
                { 201, 202, 203, 205, 206, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 20100, 201000,
                    201001, 2010000, 2020000, 2020001 }
        };
        for (int i = 0; i < arrays.length; i++) {
            for (int j = i + 1; j < arrays.length; j++) {
                long[] tempArray1 = arrays[i];
                long[] tempArray2 = arrays[j];
                HashSet<Long> unionSet = new HashSet<Long>();
                for (int k = 0; k < tempArray1.length; k++) {
                    unionSet.add(tempArray1[k]);
                }
                for (int k = 0; k < tempArray2.length; k++) {
                    unionSet.add(tempArray2[k]);
                }

                PrimitiveIterator.OfLong primitiveIterator1 = Arrays.stream(tempArray1).iterator();
                PrimitiveIterator.OfLong primitiveIterator2 = Arrays.stream(tempArray2).iterator();

                PrimitiveIterator.OfLong mergedItr = IteratorUtility.mergePrimitiveLongIterator(primitiveIterator1,
                        primitiveIterator2);
                ArrayList<Long> mergedArrayList = new ArrayList<Long>();
                Consumer<Long> addMethod = mergedArrayList::add;
                mergedItr.forEachRemaining(addMethod);
                int mergedListSize = mergedArrayList.size();
                assertEquals(unionSet.size(), mergedArrayList.size(), "Size of the mergedArrayList");
                assertTrue(mergedArrayList.containsAll(unionSet),
                        "mergedArrayList should contain all elements in unionSet");
                assertTrue(IntStream.range(0, mergedListSize - 1)
                        .allMatch(k -> mergedArrayList.get(k) <= mergedArrayList.get(k + 1)), "Merged Iterator should be sorted");
                assertTrue(IntStream.range(0, tempArray1.length).allMatch(k -> mergedArrayList.contains(tempArray1[k])),
                        "All elements of tempArray1 should be in mergedArrayList");
                assertTrue(IntStream.range(0, tempArray2.length).allMatch(k -> mergedArrayList.contains(tempArray2[k])),
                        "All elements of tempArray2 should be in mergedArrayList");
            }
        }
    }

    @Test
    void withItrMerge() {
        long[][] arrays = {
                { 0, 1, 2 },
                { 0, 1 },
                { 1, 2 },
                { 1, 2, 3, 5, 6, 7, 8 },
                { 1, 2, 3, 5, 6, 7, 8 },
                { 0, 1, 5 },
                { 3 },
                { 1, 2, 4, 5, 7, 8 },
                {},
                {},
                { 0 },
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 100, 1000, 1001, 10000, 20000, 20001 },
                { 201, 202, 203, 205, 206, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 20100, 201000,
                    201001, 2010000, 2020000, 2020001 }
        };
        for (int i = 0; i < arrays.length; i++) {
            for (int j = i + 1; j < arrays.length; j++) {
                long[] tempArray1 = arrays[i];
                ArrayList<Long> tempArrayList1 = new ArrayList<Long>();
                IntStream.range(0, tempArray1.length).forEach((k) -> tempArrayList1.add(tempArray1[k]));
                long[] tempArray2 = arrays[j];
                ArrayList<Long> tempArrayList2 = new ArrayList<Long>();
                IntStream.range(0, tempArray2.length).forEach((k) -> tempArrayList2.add(tempArray2[k]));
                HashSet<Long> unionSet = new HashSet<Long>();
                unionSet.addAll(tempArrayList1);
                unionSet.addAll(tempArrayList2);

                Iterator<Long> itr1 = tempArrayList1.iterator();
                Iterator<Long> itr2 = tempArrayList2.iterator();

                Iterator<Long> mergedItr = IteratorUtility.mergeIteratorsForPrimitiveLongIterator(itr1, itr2,
                        Long::compare, (l) -> l);
                ArrayList<Long> mergedArrayList = new ArrayList<Long>();
                Consumer<Long> addMethod = mergedArrayList::add;
                mergedItr.forEachRemaining(addMethod);
                int mergedListSize = mergedArrayList.size();
                assertEquals(unionSet.size(), mergedArrayList.size(), "Size of the mergedArrayList");
                assertTrue(mergedArrayList.containsAll(unionSet),
                        "mergedArrayList should contain all elements in unionSet");
                assertTrue(IntStream.range(0, mergedListSize - 1)
                        .allMatch(k -> mergedArrayList.get(k) <= mergedArrayList.get(k + 1)), "Merged Iterator should be sorted");
                assertTrue(IntStream.range(0, tempArray1.length).allMatch(k -> mergedArrayList.contains(tempArray1[k])),
                        "All elements of tempArray1 should be in mergedArrayList");
                assertTrue(IntStream.range(0, tempArray2.length).allMatch(k -> mergedArrayList.contains(tempArray2[k])),
                        "All elements of tempArray2 should be in mergedArrayList");
            }
        }
    }
}
