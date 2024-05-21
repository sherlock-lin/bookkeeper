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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap.LongLongFunction;
import org.junit.jupiter.api.Test;

/**
 * Test the ConcurrentLongLongHashMap class.
 */
@SuppressWarnings("deprecation")
class ConcurrentLongLongHashMapTest {

    @Test
    void constructor() {
        try {
            ConcurrentLongLongHashMap.newBuilder()
                    .expectedItems(0)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ConcurrentLongLongHashMap.newBuilder()
                    .expectedItems(16)
                    .concurrencyLevel(0)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ConcurrentLongLongHashMap.newBuilder()
                    .expectedItems(4)
                    .concurrencyLevel(8)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    void simpleInsertions() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .build();

        assertTrue(map.isEmpty());
        assertEquals(-1, map.put(1, 11));
        assertFalse(map.isEmpty());

        assertEquals(-1, map.put(2, 22));
        assertEquals(-1, map.put(3, 33));

        assertEquals(3, map.size());

        assertEquals(11, map.get(1));
        assertEquals(3, map.size());

        assertEquals(11, map.remove(1));
        assertEquals(2, map.size());
        assertEquals(-1, map.get(1));
        assertEquals(-1, map.get(5));
        assertEquals(2, map.size());

        assertEquals(-1, map.put(1, 11));
        assertEquals(3, map.size());
        assertEquals(11, map.put(1, 111));
        assertEquals(3, map.size());
    }

    @Test
    void clear() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertEquals(4, map.capacity());

        assertEquals(-1, map.put(1, 1));
        assertEquals(-1, map.put(2, 2));
        assertEquals(-1, map.put(3, 3));

        assertEquals(8, map.capacity());
        map.clear();
        assertEquals(4, map.capacity());
    }

    @Test
    void expandAndShrink() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertEquals(-1, map.put(1, 1));
        assertEquals(-1, map.put(2, 2));
        assertEquals(-1, map.put(3, 3));

        // expand hashmap
        assertEquals(8, map.capacity());

        assertTrue(map.remove(1, 1));
        // not shrink
        assertEquals(8, map.capacity());
        assertTrue(map.remove(2, 2));
        // shrink hashmap
        assertEquals(4, map.capacity());

        // expand hashmap
        assertEquals(-1, map.put(4, 4));
        assertEquals(-1, map.put(5, 5));
        assertEquals(8, map.capacity());

        //verify that the map does not keep shrinking at every remove() operation
        assertEquals(-1, map.put(6, 6));
        assertTrue(map.remove(6, 6));
        assertEquals(8, map.capacity());
    }

    @Test
    void concurrentExpandAndShrinkAndGet()  throws Throwable {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertEquals(4, map.capacity());

        ExecutorService executor = Executors.newCachedThreadPool();
        final int readThreads = 16;
        final int writeThreads = 1;
        final int n = 1_000;
        CyclicBarrier barrier = new CyclicBarrier(writeThreads + readThreads);
        Future<?> future = null;
        AtomicReference<Exception> ex = new AtomicReference<>();

        for (int i = 0; i < readThreads; i++) {
            executor.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                while (true) {
                    try {
                        map.get(1);
                    } catch (Exception e) {
                        ex.set(e);
                    }
                }
            });
        }
        map.put(1, 11);
        future = executor.submit(() -> {
            try {
                barrier.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            for (int i = 0; i < n; i++) {
                // expand hashmap
                map.put(2, 22);
                map.put(3, 33);
                assertEquals(8, map.capacity());

                // shrink hashmap
                map.remove(2, 22);
                map.remove(3, 33);
                assertEquals(4, map.capacity());
            }
        });

        future.get();
        assertNull(ex.get());
        // shut down pool
        executor.shutdown();
    }

    @Test
    void expandShrinkAndClear() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        final long initCapacity = map.capacity();
        assertEquals(4, map.capacity());
        assertEquals(-1, map.put(1, 1));
        assertEquals(-1, map.put(2, 2));
        assertEquals(-1, map.put(3, 3));

        // expand hashmap
        assertEquals(8, map.capacity());

        assertTrue(map.remove(1, 1));
        // not shrink
        assertEquals(8, map.capacity());
        assertTrue(map.remove(2, 2));
        // shrink hashmap
        assertEquals(4, map.capacity());

        assertTrue(map.remove(3, 3));
        // Will not shrink the hashmap again because shrink capacity is less than initCapacity
        // current capacity is equal than the initial capacity
        assertEquals(map.capacity(), initCapacity);
        map.clear();
        // after clear, because current capacity is equal than the initial capacity, so not shrinkToInitCapacity
        assertEquals(map.capacity(), initCapacity);
    }

    @Test
    void remove() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder().build();

        assertTrue(map.isEmpty());
        assertEquals(-1, map.put(1, 11));
        assertFalse(map.isEmpty());

        assertFalse(map.remove(0, 0));
        assertFalse(map.remove(1, 111));

        assertFalse(map.isEmpty());
        assertTrue(map.remove(1, 11));
        assertTrue(map.isEmpty());
    }

    @Test
    void negativeUsedBucketCount() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();

        map.put(0, 0);
        assertEquals(1, map.getUsedBucketCount());
        map.put(0, 1);
        assertEquals(1, map.getUsedBucketCount());
        map.remove(0);
        assertEquals(0, map.getUsedBucketCount());
        map.remove(0);
        assertEquals(0, map.getUsedBucketCount());
    }

    @Test
    void rehashing() {
        int n = 16;
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(n / 2)
                .concurrencyLevel(1)
                .build();
        assertEquals(map.capacity(), n);
        assertEquals(0, map.size());

        for (int i = 0; i < n; i++) {
            map.put(i, i);
        }

        assertEquals(map.capacity(), 2 * n);
        assertEquals(map.size(), n);
    }

    @Test
    void rehashingWithDeletes() {
        int n = 16;
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(n / 2)
                .concurrencyLevel(1)
                .build();
        assertEquals(map.capacity(), n);
        assertEquals(0, map.size());

        for (int i = 0; i < n / 2; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < n / 2; i++) {
            map.remove(i);
        }

        for (int i = n; i < (2 * n); i++) {
            map.put(i, i);
        }

        assertEquals(map.capacity(), 2 * n);
        assertEquals(map.size(), n);
    }

    @Test
    void concurrentInsertions() throws Throwable {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder().build();
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int n = 100_000;
        long value = 55;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                for (int j = 0; j < n; j++) {
                    long key = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
                    // Ensure keys are uniques
                    key -= key % (threadIdx + 1);

                    map.put(key, value);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals(map.size(), n * nThreads);

        executor.shutdown();
    }

    @Test
    void concurrentInsertionsAndReads() throws Throwable {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder().build();
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int n = 100_000;
        final long value = 55;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                for (int j = 0; j < n; j++) {
                    long key = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
                    // Ensure keys are uniques
                    key -= key % (threadIdx + 1);

                    map.put(key, value);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals(map.size(), n * nThreads);

        executor.shutdown();
    }

    @Test
    void iteration() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder().build();

        assertEquals(map.keys(), Collections.emptyList());
        assertEquals(map.values(), Collections.emptyList());

        map.put(0, 0);

        assertEquals(map.keys(), Lists.newArrayList(0L));
        assertEquals(map.values(), Lists.newArrayList(0L));

        map.remove(0);

        assertEquals(map.keys(), Collections.emptyList());
        assertEquals(map.values(), Collections.emptyList());

        map.put(0, 0);
        map.put(1, 11);
        map.put(2, 22);

        List<Long> keys = map.keys();
        Collections.sort(keys);
        assertEquals(keys, Lists.newArrayList(0L, 1L, 2L));

        List<Long> values = map.values();
        Collections.sort(values);
        assertEquals(values, Lists.newArrayList(0L, 11L, 22L));

        map.put(1, 111);

        keys = map.keys();
        Collections.sort(keys);
        assertEquals(keys, Lists.newArrayList(0L, 1L, 2L));

        values = map.values();
        Collections.sort(values);
        assertEquals(values, Lists.newArrayList(0L, 22L, 111L));

        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    void hashConflictWithDeletion() {
        final int buckets = 16;
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(buckets)
                .concurrencyLevel(1)
                .build();

        // Pick 2 keys that fall into the same bucket
        long key1 = 1;
        long key2 = 27;

        int bucket1 = ConcurrentLongLongHashMap.signSafeMod(ConcurrentLongLongHashMap.hash(key1), buckets);
        int bucket2 = ConcurrentLongLongHashMap.signSafeMod(ConcurrentLongLongHashMap.hash(key2), buckets);
        assertEquals(bucket1, bucket2);

        final long value1 = 1;
        final long value2 = 2;
        final long value1Overwrite = 3;
        final long value2Overwrite = 3;

        assertEquals(-1, map.put(key1, value1));
        assertEquals(-1, map.put(key2, value2));
        assertEquals(2, map.size());

        assertEquals(map.remove(key1), value1);
        assertEquals(1, map.size());

        assertEquals(-1, map.put(key1, value1Overwrite));
        assertEquals(2, map.size());

        assertEquals(map.remove(key1), value1Overwrite);
        assertEquals(1, map.size());

        assertEquals(map.put(key2, value2Overwrite), value2);
        assertEquals(map.get(key2), value2Overwrite);

        assertEquals(1, map.size());
        assertEquals(map.remove(key2), value2Overwrite);
        assertTrue(map.isEmpty());
    }

    @Test
    void putIfAbsent() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder().build();
        assertEquals(-1, map.putIfAbsent(1, 11));
        assertEquals(11, map.get(1));

        assertEquals(11, map.putIfAbsent(1, 111));
        assertEquals(11, map.get(1));
    }

    @Test
    void computeIfAbsent() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();
        AtomicLong counter = new AtomicLong();
        LongLongFunction provider = new LongLongFunction() {
            public long apply(long key) {
                return counter.getAndIncrement();
            }
        };

        assertEquals(0, map.computeIfAbsent(0, provider));
        assertEquals(0, map.get(0));

        assertEquals(1, map.computeIfAbsent(1, provider));
        assertEquals(1, map.get(1));

        assertEquals(1, map.computeIfAbsent(1, provider));
        assertEquals(1, map.get(1));

        assertEquals(2, map.computeIfAbsent(2, provider));
        assertEquals(2, map.get(2));
    }

    @Test
    void addAndGet() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();

        assertEquals(0, map.addAndGet(0, 0));
        assertTrue(map.containsKey(0));
        assertEquals(0, map.get(0));

        assertFalse(map.containsKey(5));

        assertEquals(5, map.addAndGet(0, 5));
        assertEquals(5, map.get(0));

        assertEquals(6, map.addAndGet(0, 1));
        assertEquals(6, map.get(0));

        assertEquals(4, map.addAndGet(0, -2));
        assertEquals(4, map.get(0));

        // Cannot bring to value to negative
        try {
            map.addAndGet(0, -5);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }
        assertEquals(4, map.get(0));
    }

    @Test
    void reduceUnnecessaryExpansions() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .build();
        map.put(1L, 1L);
        map.put(2L, 2L);
        map.put(3L, 3L);
        map.put(4L, 4L);

        map.remove(1L);
        map.remove(2L);
        map.remove(3L);
        map.remove(4L);
        assertEquals(0, map.getUsedBucketCount());
    }


    @Test
    void removeIf() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();

        map.put(1L, 1L);
        map.put(2L, 2L);
        map.put(3L, 3L);
        map.put(4L, 4L);

        map.removeIf(key -> key < 3);
        assertFalse(map.containsKey(1L));
        assertFalse(map.containsKey(2L));
        assertTrue(map.containsKey(3L));
        assertTrue(map.containsKey(4L));
        assertEquals(2, map.size());
    }

    @Test
    void removeIfValue() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();

        map.put(1L, 1L);
        map.put(2L, 2L);
        map.put(3L, 1L);
        map.put(4L, 2L);

        map.removeIf((key, value) -> value < 2);
        assertFalse(map.containsKey(1L));
        assertTrue(map.containsKey(2L));
        assertFalse(map.containsKey(3L));
        assertTrue(map.containsKey(4L));
        assertEquals(2, map.size());
    }

    @Test
    void ivalidKeys() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();

        try {
            map.put(-5, 4);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            map.get(-1);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            map.containsKey(-1);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            map.putIfAbsent(-1, 1);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            map.computeIfAbsent(-1, new LongLongFunction() {
                @Override
                public long apply(long key) {
                    return 1;
                }
            });
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    void asMap() {
        ConcurrentLongLongHashMap lmap = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();
        lmap.put(1, 11);
        lmap.put(2, 22);
        lmap.put(3, 33);

        Map<Long, Long> map = Maps.newTreeMap();
        map.put(1L, 11L);
        map.put(2L, 22L);
        map.put(3L, 33L);

        assertEquals(map, lmap.asMap());
    }

    @Test
    void sizeInBytes() {
        ConcurrentLongLongHashMap lmap = new ConcurrentLongLongHashMap(4, 2);
        assertEquals(128, lmap.sizeInBytes());
        lmap.put(1, 1);
        assertEquals(128, lmap.sizeInBytes());
        lmap.put(2, 2);
        assertEquals(128, lmap.sizeInBytes());
        lmap.put(3, 3);
        assertEquals(128, lmap.sizeInBytes());
        lmap.put(4, 4);
        assertEquals(192, lmap.sizeInBytes());
        lmap.put(5, 5);
        assertEquals(192, lmap.sizeInBytes());
        lmap.put(6, 6);
        assertEquals(256, lmap.sizeInBytes());
        lmap.put(7, 7);
        assertEquals(256, lmap.sizeInBytes());
    }
}
