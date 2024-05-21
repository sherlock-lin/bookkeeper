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
package org.apache.bookkeeper.util.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Test for SynchronizedHashMultiMap.
 */
class SynchronizedHashMultiMapTest {
    @Test
    void getAnyKey() {
        SynchronizedHashMultiMap<Integer, Integer> map = new SynchronizedHashMultiMap<>();
        assertFalse(map.getAnyKey().isPresent());

        map.put(1, 2);
        assertEquals(map.getAnyKey().get(), Integer.valueOf(1));

        map.put(1, 3);
        assertEquals(map.getAnyKey().get(), Integer.valueOf(1));

        map.put(2, 4);
        int res = map.getAnyKey().get();
        assertTrue(res == 1 || res == 2);

        map.removeIf((k, v) -> k == 1);
        assertEquals(map.getAnyKey().get(), Integer.valueOf(2));
    }

    @Test
    void removeAny() {
        SynchronizedHashMultiMap<Integer, Integer> map = new SynchronizedHashMultiMap<>();
        assertFalse(map.removeAny(1).isPresent());

        map.put(1, 2);
        map.put(1, 3);
        map.put(2, 4);
        map.put(2, 4);

        Optional<Integer> v = map.removeAny(1);
        int firstVal = v.get();
        assertTrue(firstVal == 2 || firstVal == 3);

        v = map.removeAny(1);
        int secondVal = v.get();
        assertTrue(secondVal == 2 || secondVal == 3);
        assertNotEquals(secondVal, firstVal);

        v = map.removeAny(2);
        assertTrue(v.isPresent());
        assertEquals(v.get(), Integer.valueOf(4));

        assertFalse(map.removeAny(1).isPresent());
        assertFalse(map.removeAny(2).isPresent());
        assertFalse(map.removeAny(3).isPresent());
    }

    @Test
    void removeIf() {
        SynchronizedHashMultiMap<Integer, Integer> map = new SynchronizedHashMultiMap<>();
        assertEquals(0, map.removeIf((k, v) -> true));

        map.put(1, 2);
        map.put(1, 3);
        map.put(2, 4);
        map.put(2, 4);

        assertEquals(1, map.removeIf((k, v) -> v == 4));
        assertEquals(2, map.removeIf((k, v) -> k == 1));

        map.put(1, 2);
        map.put(1, 3);
        map.put(2, 4);

        assertEquals(0, map.removeIf((k, v) -> false));
        assertEquals(3, map.removeIf((k, v) -> true));
    }
}
