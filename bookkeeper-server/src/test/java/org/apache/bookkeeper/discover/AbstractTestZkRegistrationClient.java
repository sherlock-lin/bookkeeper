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
package org.apache.bookkeeper.discover;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.collect;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.common.testing.MoreAsserts.assertSetEquals;
import static org.apache.bookkeeper.discover.ZKRegistrationClient.ZK_CONNECT_BACKOFF_MS;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.COOKIE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.ZKException;
import org.apache.bookkeeper.common.testing.executors.MockExecutorController;
import org.apache.bookkeeper.discover.RegistrationClient.RegistrationListener;
import org.apache.bookkeeper.discover.ZKRegistrationClient.WatchTask;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.zookeeper.MockZooKeeperTestCase;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Unit test of {@link RegistrationClient}.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
@Slf4j
public abstract class AbstractTestZkRegistrationClient extends MockZooKeeperTestCase {


    
    public final String runtime;

    private String ledgersPath;
    private String regPath;
    private String regAllPath;
    private String regReadonlyPath;
    private ZKRegistrationClient zkRegistrationClient;
    private ScheduledExecutorService mockExecutor;
    private MockExecutorController controller;

    private final boolean bookieAddressChangeTracking;

    public AbstractTestZkRegistrationClient(boolean bookieAddressChangeTracking) {
        this.bookieAddressChangeTracking = bookieAddressChangeTracking;
    }


    @Override
    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        Optional<Method> testMethod = testInfo.getTestMethod();
        if (testMethod.isPresent()) {
            this.runtime = testMethod.get().getName();
        }
        super.setup();

        this.ledgersPath = "/" + runtime;
        this.regPath = ledgersPath + "/" + AVAILABLE_NODE;
        this.regAllPath = ledgersPath + "/" + COOKIE_NODE;
        this.regReadonlyPath = regPath + "/" + READONLY;
        this.mockExecutor = mock(ScheduledExecutorService.class);
        this.controller = new MockExecutorController()
                .controlExecute(mockExecutor)
                .controlSubmit(mockExecutor)
                .controlSchedule(mockExecutor)
                .controlScheduleAtFixedRate(mockExecutor, 10);
        this.zkRegistrationClient = new ZKRegistrationClient(
                mockZk,
                ledgersPath,
                mockExecutor,
                bookieAddressChangeTracking
        );
        assertEquals(bookieAddressChangeTracking, zkRegistrationClient.isBookieAddressTracking());
    }

    @AfterEach
    public void teardown() throws Exception {
        super.teardown();

        if (null != zkRegistrationClient) {
            zkRegistrationClient.close();
        }
    }

    private static Set<BookieId> prepareNBookies(int num) {
        Set<BookieId> bookies = new HashSet<>();
        for (int i = 0; i < num; i++) {
            bookies.add(new BookieSocketAddress("127.0.0.1", 3181 + i).toBookieId());
        }
        return bookies;
    }

    private void prepareReadBookieServiceInfo(BookieId address, boolean readonly) throws Exception {
        if (readonly) {
            mockZkGetData(regPath + "/" + address.toString(),
                    zkRegistrationClient.isBookieAddressTracking(),
                    Code.NONODE.intValue(),
                    new byte[]{},
                    new Stat());
            mockZkGetData(regReadonlyPath + "/" + address,
                    zkRegistrationClient.isBookieAddressTracking(),
                    Code.OK.intValue(),
                    new byte[]{},
                    new Stat());
        } else {
            mockZkGetData(regPath + "/" + address.toString(),
                    zkRegistrationClient.isBookieAddressTracking(),
                    Code.OK.intValue(),
                    new byte[]{},
                    new Stat());
            mockZkGetData(regReadonlyPath + "/" + address,
                    zkRegistrationClient.isBookieAddressTracking(),
                    Code.NONODE.intValue(),
                    new byte[]{},
                    new Stat());
        }
    }

    @Test
    public void getWritableBookies() throws Exception {
        Set<BookieId> addresses = prepareNBookies(10);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }

        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);
        mockGetChildren(
                regPath, false,
                Code.OK.intValue(), children, stat);

        Versioned<Set<BookieId>> result =
                result(zkRegistrationClient.getWritableBookies());

        assertEquals(new LongVersion(1234), result.getVersion());
        assertSetEquals(
                addresses, result.getValue());
    }

    @Test
    public void getAllBookies() throws Exception {
        Set<BookieId> addresses = prepareNBookies(10);
        List<String> children = Lists.newArrayList();

        int i = 0;
        for (BookieId address : addresses) {
            children.add(address.toString());
            boolean readonly = i++ % 2 == 0;
            prepareReadBookieServiceInfo(address, readonly);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);
        mockGetChildren(
                regAllPath, false,
                Code.OK.intValue(), children, stat);

        Versioned<Set<BookieId>> result =
                result(zkRegistrationClient.getAllBookies());

        assertEquals(new LongVersion(1234), result.getVersion());
        assertSetEquals(
                addresses, result.getValue());
    }

    @Test
    public void getReadOnlyBookies() throws Exception {
        Set<BookieId> addresses = prepareNBookies(10);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);
        mockGetChildren(
                regReadonlyPath, false,
                Code.OK.intValue(), children, stat);

        Versioned<Set<BookieId>> result =
                result(zkRegistrationClient.getReadOnlyBookies());

        assertEquals(new LongVersion(1234), result.getVersion());
        assertSetEquals(
                addresses, result.getValue());
    }

    @Test
    public void getWritableBookiesFailure() throws Exception {
        mockGetChildren(
                regPath, false,
                Code.NONODE.intValue(), null, null);

        try {
            result(zkRegistrationClient.getWritableBookies());
            fail("Should fail to get writable bookies");
        } catch (ZKException zke) {
            // expected to throw zookeeper exception
        }
    }

    @Test
    public void getAllBookiesFailure() throws Exception {
        mockGetChildren(
                regAllPath, false,
                Code.NONODE.intValue(), null, null);

        try {
            result(zkRegistrationClient.getAllBookies());
            fail("Should fail to get all bookies");
        } catch (ZKException zke) {
            // expected to throw zookeeper exception
        }
    }

    @Test
    public void getReadOnlyBookiesFailure() throws Exception {
        mockGetChildren(
                regReadonlyPath, false,
                Code.NONODE.intValue(), null, null);

        try {
            result(zkRegistrationClient.getReadOnlyBookies());
            fail("Should fail to get writable bookies");
        } catch (ZKException zke) {
            // expected to throw zookeeper exception
        }
    }

    @Test
    public void watchWritableBookiesSuccess() throws Exception {
        testWatchBookiesSuccess(true);
    }

    @Test
    public void watchReadonlyBookiesSuccess() throws Exception {
        testWatchBookiesSuccess(false);
    }

    @SuppressWarnings("unchecked")
    private void testWatchBookiesSuccess(boolean isWritable)
            throws Exception {
        //
        // 1. test watch bookies with a listener
        //

        LinkedBlockingQueue<Versioned<Set<BookieId>>> updates =
                spy(new LinkedBlockingQueue<>());
        RegistrationListener listener = bookies -> {
            try {
                updates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted on enqueue bookie updates", e);
            }
        };

        Set<BookieId> addresses = prepareNBookies(10);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, !isWritable);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(
                isWritable ? regPath : regReadonlyPath,
                true,
                Code.OK.intValue(), children, stat);

        if (isWritable) {
            result(zkRegistrationClient.watchWritableBookies(listener));
        } else {
            result(zkRegistrationClient.watchReadOnlyBookies(listener));
        }

        Versioned<Set<BookieId>> update = updates.take();
        verify(updates, times(1)).put(any(Versioned.class));
        assertEquals(new LongVersion(1234), update.getVersion());
        assertSetEquals(
                addresses, update.getValue());

        verify(mockZk, times(1))
                .getChildren(anyString(), any(Watcher.class), any(Children2Callback.class), any());

        //
        // 2. test watch bookies with a second listener. the second listener returns cached bookies
        //    without calling `getChildren` again
        //

        // register another listener
        LinkedBlockingQueue<Versioned<Set<BookieId>>> secondUpdates =
                spy(new LinkedBlockingQueue<>());
        RegistrationListener secondListener = bookies -> {
            try {
                secondUpdates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted on enqueue bookie updates", e);
            }
        };
        if (isWritable) {
            result(zkRegistrationClient.watchWritableBookies(secondListener));
        } else {
            result(zkRegistrationClient.watchReadOnlyBookies(secondListener));
        }
        Versioned<Set<BookieId>> secondListenerUpdate = secondUpdates.take();
        // first listener will not be notified with any update
        verify(updates, times(1)).put(any(Versioned.class));
        // second listener will receive same update as the first listener received before
        verify(secondUpdates, times(1)).put(any(Versioned.class));
        assertSame(update.getVersion(), secondListenerUpdate.getVersion());
        assertSame(update.getValue(), secondListenerUpdate.getValue());

        // the second listener will return the cached value without issuing another getChildren call
        verify(mockZk, times(1))
                .getChildren(anyString(), any(Watcher.class), any(Children2Callback.class), any());

        //
        // 3. simulate session expire, it will trigger watcher to refetch bookies again.
        //    but since there is no updates on bookies, the registered listeners will not be notified.
        //

        notifyWatchedEvent(
                EventType.None,
                KeeperState.Expired,
                isWritable ? regPath : regReadonlyPath);

        // if session expires, the watcher task will get into backoff state
        controller.advance(Duration.ofMillis(ZK_CONNECT_BACKOFF_MS));

        // the same updates returns, the getChildren calls increase to 2
        // but since there is no updates, so no notification is sent.
        verify(mockZk, times(2))
                .getChildren(anyString(), any(Watcher.class), any(Children2Callback.class), any());
        assertNull(updates.poll());
        // both listener and secondListener will not receive any old update
        verify(updates, times(1)).put(any(Versioned.class));
        verify(secondUpdates, times(1)).put(any(Versioned.class));

        //
        // 4. notify with new bookies. both listeners will be notified with new bookies.
        //

        Set<BookieId> newAddresses = prepareNBookies(20);
        List<String> newChildren = Lists.newArrayList();
        for (BookieId address : newAddresses) {
            newChildren.add(address.toString());
            prepareReadBookieServiceInfo(address, !isWritable);
        }
        Stat newStat = mock(Stat.class);
        when(newStat.getCversion()).thenReturn(1235);

        mockGetChildren(
                isWritable ? regPath : regReadonlyPath,
                true,
                Code.OK.intValue(), newChildren, newStat);

        // trigger watcher
        notifyWatchedEvent(
                EventType.NodeChildrenChanged,
                KeeperState.SyncConnected,
                isWritable ? regPath : regReadonlyPath);

        update = updates.take();
        assertEquals(new LongVersion(1235), update.getVersion());
        assertSetEquals(
                newAddresses, update.getValue());
        secondListenerUpdate = secondUpdates.take();
        assertSame(update.getVersion(), secondListenerUpdate.getVersion());
        assertSame(update.getValue(), secondListenerUpdate.getValue());

        verify(mockZk, times(3))
                .getChildren(anyString(), any(Watcher.class), any(Children2Callback.class), any());
        verify(updates, times(2)).put(any(Versioned.class));
        verify(secondUpdates, times(2)).put(any(Versioned.class));

        //
        // 5. unwatch the second listener and notify with new bookies again. only first listener will
        //    be notified with new bookies.
        //

        newAddresses = prepareNBookies(25);
        newChildren.clear();
        newChildren = Lists.newArrayList();
        for (BookieId address : newAddresses) {
            newChildren.add(address.toString());
            prepareReadBookieServiceInfo(address, !isWritable);
        }
        newStat = mock(Stat.class);
        when(newStat.getCversion()).thenReturn(1236);

        mockGetChildren(
                isWritable ? regPath : regReadonlyPath,
                true,
                Code.OK.intValue(), newChildren, newStat);

        if (isWritable) {
            assertEquals(2, zkRegistrationClient.getWatchWritableBookiesTask().getNumListeners());
            zkRegistrationClient.unwatchWritableBookies(secondListener);
            assertEquals(1, zkRegistrationClient.getWatchWritableBookiesTask().getNumListeners());
        } else {
            assertEquals(2, zkRegistrationClient.getWatchReadOnlyBookiesTask().getNumListeners());
            zkRegistrationClient.unwatchReadOnlyBookies(secondListener);
            assertEquals(1, zkRegistrationClient.getWatchReadOnlyBookiesTask().getNumListeners());
        }

        // trigger watcher
        notifyWatchedEvent(
                EventType.NodeChildrenChanged,
                KeeperState.SyncConnected,
                isWritable ? regPath : regReadonlyPath);

        update = updates.take();
        assertEquals(new LongVersion(1236), update.getVersion());
        assertSetEquals(
                newAddresses, update.getValue());
        secondListenerUpdate = secondUpdates.poll();
        assertNull(secondListenerUpdate);

        verify(mockZk, times(4))
                .getChildren(anyString(), any(Watcher.class), any(Children2Callback.class), any());
        verify(updates, times(3)).put(any(Versioned.class));
        verify(secondUpdates, times(2)).put(any(Versioned.class));

        //
        // 6. unwatch the first listener. the watch task will be closed and zk watcher will be removed.
        //
        //

        WatchTask expectedWatcher;
        if (isWritable) {
            expectedWatcher = zkRegistrationClient.getWatchWritableBookiesTask();
            assertFalse(expectedWatcher.isClosed());
            zkRegistrationClient.unwatchWritableBookies(listener);
            assertNull(zkRegistrationClient.getWatchWritableBookiesTask());
        } else {
            expectedWatcher = zkRegistrationClient.getWatchReadOnlyBookiesTask();
            assertFalse(expectedWatcher.isClosed());
            zkRegistrationClient.unwatchReadOnlyBookies(listener);
            assertNull(zkRegistrationClient.getWatchReadOnlyBookiesTask());
        }
        // the watch task will not be closed since there is still a listener
        assertTrue(expectedWatcher.isClosed());
    }

    @Test
    public void watchWritableBookiesTwice() throws Exception {
        testWatchBookiesTwice(true);
    }

    @Test
    public void watchReadonlyBookiesTwice() throws Exception {
        testWatchBookiesTwice(false);
    }

    private void testWatchBookiesTwice(boolean isWritable)
            throws Exception {
        int zkCallbackDelayMs = 100;

        Set<BookieId> addresses = prepareNBookies(10);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, !isWritable);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(
                isWritable ? regPath : regReadonlyPath,
                true,
                Code.OK.intValue(), children, stat, zkCallbackDelayMs);

        CompletableFuture<Versioned<Set<BookieId>>> firstResult = new CompletableFuture<>();
        RegistrationListener firstListener = bookies -> firstResult.complete(bookies);

        CompletableFuture<Versioned<Set<BookieId>>> secondResult = new CompletableFuture<>();
        RegistrationListener secondListener = bookies -> secondResult.complete(bookies);

        List<CompletableFuture<Void>> watchFutures = Lists.newArrayListWithExpectedSize(2);
        if (isWritable) {
            watchFutures.add(zkRegistrationClient.watchWritableBookies(firstListener));
            watchFutures.add(zkRegistrationClient.watchWritableBookies(secondListener));
        } else {
            watchFutures.add(zkRegistrationClient.watchReadOnlyBookies(firstListener));
            watchFutures.add(zkRegistrationClient.watchReadOnlyBookies(secondListener));
        }

        // trigger zkCallbackExecutor to execute getChildren callback
        zkCallbackController.advance(Duration.ofMillis(zkCallbackDelayMs));

        result(collect(watchFutures));
        assertEquals(firstResult.get().getVersion(), secondResult.get().getVersion());
        assertSetEquals(firstResult.get().getValue(), secondResult.get().getValue());
    }

    @Test
    public void watchWritableBookiesFailure() throws Exception {
        testWatchBookiesFailure(true);
    }

    @Test
    public void watchReadonlyBookiesFailure() throws Exception {
        testWatchBookiesFailure(false);
    }

    private void testWatchBookiesFailure(boolean isWritable)
            throws Exception {
        int zkCallbackDelayMs = 100;

        mockGetChildren(
                isWritable ? regPath : regReadonlyPath,
                true,
                Code.NONODE.intValue(), null, null, zkCallbackDelayMs);

        CompletableFuture<Versioned<Set<BookieId>>> listenerResult = new CompletableFuture<>();
        RegistrationListener listener = bookies -> listenerResult.complete(bookies);

        CompletableFuture<Void> watchFuture;

        WatchTask watchTask;
        if (isWritable) {
            watchFuture = zkRegistrationClient.watchWritableBookies(listener);
            watchTask = zkRegistrationClient.getWatchWritableBookiesTask();
        } else {
            watchFuture = zkRegistrationClient.watchReadOnlyBookies(listener);
            watchTask = zkRegistrationClient.getWatchReadOnlyBookiesTask();
        }
        assertNotNull(watchTask);
        assertEquals(1, watchTask.getNumListeners());

        // trigger zkCallbackExecutor to execute getChildren callback
        zkCallbackController.advance(Duration.ofMillis(zkCallbackDelayMs));

        try {
            result(watchFuture);
            fail("Should fail to watch writable bookies if reg path doesn't exist");
        } catch (ZKException zke) {
            // expected
        }
        assertEquals(0, watchTask.getNumListeners());
        assertTrue(watchTask.isClosed());
        if (isWritable) {
            assertNull(zkRegistrationClient.getWatchWritableBookiesTask());
        } else {
            assertNull(zkRegistrationClient.getWatchReadOnlyBookiesTask());
        }
    }

}
