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
package org.apache.bookkeeper.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKIllegalOpException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.streaming.LedgerInputStream;
import org.apache.bookkeeper.streaming.LedgerOutputStream;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test tests read and write, synchronous and asynchronous, strings and
 * integers for a BookKeeper client. The test deployment uses a ZooKeeper server
 * and three BookKeepers.
 *
 */
public class BookieReadWriteTest extends BookKeeperClusterTestCase
    implements AddCallback, ReadCallback, ReadLastConfirmedCallback {

    // Depending on the taste, select the amount of logging
    // by decommenting one of the two lines below
    // private static final Logger LOG = Logger.getRootLogger();
    private static final Logger LOG = LoggerFactory.getLogger(BookieReadWriteTest.class);

    byte[] ledgerPassword = "aaa".getBytes();
    LedgerHandle lh, lh2;
    long ledgerId;

    // test related variables
    int numEntriesToWrite = 200;
    int maxInt = 2147483647;
    Random rng; // Random Number Generator
    ArrayList<byte[]> entries; // generated entries
    ArrayList<Integer> entriesSize;

    private final DigestType digestType;

    public BookieReadWriteTest() {
        super(3);
        this.digestType = DigestType.CRC32;
        String ledgerManagerFactory = "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory";
        // set ledger manager
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
    }

    class SyncObj {
        long lastConfirmed;
        volatile int counter;
        boolean value;
        AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        Enumeration<LedgerEntry> ls = null;

        public SyncObj() {
            counter = 0;
            lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;
            value = false;
        }

        void setReturnCode(int rc) {
            this.rc.compareAndSet(BKException.Code.OK, rc);
        }

        int getReturnCode() {
            return rc.get();
        }

        void setLedgerEntries(Enumeration<LedgerEntry> ls) {
            this.ls = ls;
        }

        Enumeration<LedgerEntry> getLedgerEntries() {
            return ls;
        }
    }

    @Test
    void openException() throws IOException, InterruptedException {
        try {
            lh = bkc.openLedger(0, digestType, ledgerPassword);
            fail("Haven't thrown exception");
        } catch (BKException e) {
            LOG.warn("Successfully thrown and caught exception:", e);
        }
    }

    /**
     * test the streaming api for reading and writing.
     *
     * @throws IOException
     */
    @Test
    void streamingClients() throws IOException, BKException, InterruptedException {
        lh = bkc.createLedger(digestType, ledgerPassword);
        // write a string so that we cna
        // create a buffer of a single bytes
        // and check for corner cases
        String toWrite = "we need to check for this string to match " + "and for the record mahadev is the best";
        LedgerOutputStream lout = new LedgerOutputStream(lh, 1);
        byte[] b = toWrite.getBytes();
        lout.write(b);
        lout.close();
        long lId = lh.getId();
        lh.close();
        // check for sanity
        lh = bkc.openLedger(lId, digestType, ledgerPassword);
        LedgerInputStream lin = new LedgerInputStream(lh, 1);
        byte[] bread = new byte[b.length];
        int read = 0;
        while (read < b.length) {
            read = read + lin.read(bread, read, b.length);
        }

        String newString = new String(bread);
        assertEquals(toWrite, newString, "these two should same");
        lin.close();
        lh.close();
        // create another ledger to write one byte at a time
        lh = bkc.createLedger(digestType, ledgerPassword);
        lout = new LedgerOutputStream(lh);
        for (int i = 0; i < b.length; i++) {
            lout.write(b[i]);
        }
        lout.close();
        lId = lh.getId();
        lh.close();
        lh = bkc.openLedger(lId, digestType, ledgerPassword);
        lin = new LedgerInputStream(lh);
        bread = new byte[b.length];
        read = 0;
        while (read < b.length) {
            read = read + lin.read(bread, read, b.length);
        }
        newString = new String(bread);
        assertEquals(toWrite, newString, "these two should be same ");
        lin.close();
        lh.close();
    }

    private void testReadWriteAsyncSingleClient(int numEntries) throws IOException {
        SyncObj sync = new SyncObj();
        try {
            // Create a ledger
            lh = bkc.createLedger(digestType, ledgerPassword);
            // bkc.initMessageDigest("SHA1");
            ledgerId = lh.getId();
            LOG.info("Ledger ID: " + lh.getId());
            for (int i = 0; i < numEntriesToWrite; i++) {
                ByteBuffer entry = ByteBuffer.allocate(4);
                entry.putInt(rng.nextInt(maxInt));
                entry.position(0);

                entries.add(entry.array());
                entriesSize.add(entry.array().length);
                lh.asyncAddEntry(entry.array(), this, sync);
            }

            // wait for all entries to be acknowledged
            synchronized (sync) {
                while (sync.counter < numEntriesToWrite) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Entries counter = " + sync.counter);
                    }
                    sync.wait();
                }
                assertEquals(BKException.Code.OK, sync.getReturnCode(), "Error adding");
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("*** WRITE COMPLETE ***");
            }
            // close ledger
            lh.close();

            // *** WRITING PART COMPLETE // READ PART BEGINS ***

            // open ledger
            lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Number of entries written: " + (lh.getLastAddConfirmed() + 1));
            }
            assertEquals(lh.getLastAddConfirmed(), (numEntriesToWrite - 1), "Verifying number of entries written");

            // read entries
            lh.asyncReadEntries(0, numEntriesToWrite - 1, this, sync);

            synchronized (sync) {
                while (!sync.value) {
                    sync.wait();
                }
                assertEquals(BKException.Code.OK, sync.getReturnCode(), "Error reading");
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("*** READ COMPLETE ***");
            }

            // at this point, Enumeration<LedgerEntry> ls is filled with the returned
            // values
            int i = 0;
            Enumeration<LedgerEntry> ls = sync.getLedgerEntries();
            while (ls.hasMoreElements()) {
                ByteBuffer origbb = ByteBuffer.wrap(entries.get(i));
                Integer origEntry = origbb.getInt();
                byte[] entry = ls.nextElement().getEntry();
                ByteBuffer result = ByteBuffer.wrap(entry);
                Integer retrEntry = result.getInt();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Length of result: " + result.capacity());
                    LOG.debug("Original entry: " + origEntry);
                    LOG.debug("Retrieved entry: " + retrEntry);
                }
                assertEquals(origEntry, retrEntry, "Checking entry " + i + " for equality");
                assertTrue(entry.length == entriesSize.get(i), "Checking entry " + i + " for size");
                i++;
            }
            assertEquals(i, numEntriesToWrite, "Checking number of read entries");

            lh.close();
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }

    @Test
    void readWriteAsyncSingleClient200() throws IOException {
        testReadWriteAsyncSingleClient(200);
    }

    /**
     * Check that the add api with offset and length work correctly.
     * First try varying the offset. Then the length with a fixed non-zero
     * offset.
     */
    @Test
    void readWriteRangeAsyncSingleClient() throws IOException {
        SyncObj sync = new SyncObj();
        try {
            // Create a ledger
            lh = bkc.createLedger(digestType, ledgerPassword);
            // bkc.initMessageDigest("SHA1");
            ledgerId = lh.getId();
            LOG.info("Ledger ID: " + lh.getId());
            byte[] bytes = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'};

            lh.asyncAddEntry(bytes, 0, bytes.length, this, sync);
            lh.asyncAddEntry(bytes, 0, 4, this, sync); // abcd
            lh.asyncAddEntry(bytes, 3, 4, this, sync); // defg
            lh.asyncAddEntry(bytes, 3, (bytes.length - 3), this, sync); // defghi
            int numEntries = 4;

            // wait for all entries to be acknowledged
            synchronized (sync) {
                while (sync.counter < numEntries) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Entries counter = " + sync.counter);
                    }
                    sync.wait();
                }
                assertEquals(BKException.Code.OK, sync.getReturnCode(), "Error adding");
            }

            try {
                lh.asyncAddEntry(bytes, -1, bytes.length, this, sync);
                fail("Shouldn't be able to use negative offset");
            } catch (ArrayIndexOutOfBoundsException aiob) {
                // expected
            }
            try {
                lh.asyncAddEntry(bytes, 0, bytes.length + 1, this, sync);
                fail("Shouldn't be able to use that much length");
            } catch (ArrayIndexOutOfBoundsException aiob) {
                // expected
            }
            try {
                lh.asyncAddEntry(bytes, -1, bytes.length + 2, this, sync);
                fail("Shouldn't be able to use negative offset "
                     + "with that much length");
            } catch (ArrayIndexOutOfBoundsException aiob) {
                // expected
            }
            try {
                lh.asyncAddEntry(bytes, 4, -3, this, sync);
                fail("Shouldn't be able to use negative length");
            } catch (ArrayIndexOutOfBoundsException aiob) {
                // expected
            }
            try {
                lh.asyncAddEntry(bytes, -4, -3, this, sync);
                fail("Shouldn't be able to use negative offset & length");
            } catch (ArrayIndexOutOfBoundsException aiob) {
                // expected
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("*** WRITE COMPLETE ***");
            }
            // close ledger
            lh.close();

            // *** WRITING PART COMPLETE // READ PART BEGINS ***

            // open ledger
            lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Number of entries written: " + (lh.getLastAddConfirmed() + 1));
            }
            assertEquals(lh.getLastAddConfirmed(), (numEntries - 1), "Verifying number of entries written");

            // read entries
            lh.asyncReadEntries(0, numEntries - 1, this, sync);

            synchronized (sync) {
                while (!sync.value) {
                    sync.wait();
                }
                assertEquals(BKException.Code.OK, sync.getReturnCode(), "Error reading");
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("*** READ COMPLETE ***");
            }

            // at this point, Enumeration<LedgerEntry> ls is filled with the returned
            // values
            int i = 0;
            Enumeration<LedgerEntry> ls = sync.getLedgerEntries();
            while (ls.hasMoreElements()) {
                byte[] expected = null;
                byte[] entry = ls.nextElement().getEntry();

                switch (i) {
                case 0:
                    expected = Arrays.copyOfRange(bytes, 0, bytes.length);
                    break;
                case 1:
                    expected = Arrays.copyOfRange(bytes, 0, 4);
                    break;
                case 2:
                    expected = Arrays.copyOfRange(bytes, 3, 3 + 4);
                    break;
                case 3:
                    expected = Arrays.copyOfRange(bytes, 3, 3 + (bytes.length - 3));
                    break;
                }
                assertNotNull(expected, "There are more checks than writes");

                String message = "Checking entry " + i + " for equality ["
                                 + new String(entry, "UTF-8") + ","
                                 + new String(expected, "UTF-8") + "]";
                assertTrue(Arrays.equals(entry, expected), message);

                i++;
            }
            assertEquals(i, numEntries, "Checking number of read entries");

            lh.close();
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }

    class ThrottleTestCallback implements ReadCallback {
        int throttle;

        ThrottleTestCallback(int threshold) {
            this.throttle = threshold;
        }

        @Override
        public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
            SyncObj sync = (SyncObj) ctx;
            sync.setLedgerEntries(seq);
            sync.setReturnCode(rc);
            synchronized (sync) {
                sync.counter += throttle;
                sync.notify();
            }
            LOG.info("Current counter: " + sync.counter);
        }
    }

    @Test
    void syncReadAsyncWriteStringsSingleClient() throws IOException {
        SyncObj sync = new SyncObj();
        LOG.info("TEST READ WRITE STRINGS MIXED SINGLE CLIENT");
        String charset = "utf-8";
        if (LOG.isDebugEnabled()) {
            LOG.debug("Default charset: " + Charset.defaultCharset());
        }
        try {
            // Create a ledger
            lh = bkc.createLedger(digestType, ledgerPassword);
            // bkc.initMessageDigest("SHA1");
            ledgerId = lh.getId();
            LOG.info("Ledger ID: " + lh.getId());
            for (int i = 0; i < numEntriesToWrite; i++) {
                int randomInt = rng.nextInt(maxInt);
                byte[] entry = Integer.toString(randomInt).getBytes(charset);
                entries.add(entry);
                lh.asyncAddEntry(entry, this, sync);
            }

            // wait for all entries to be acknowledged
            synchronized (sync) {
                while (sync.counter < numEntriesToWrite) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Entries counter = " + sync.counter);
                    }
                    sync.wait();
                }
                assertEquals(BKException.Code.OK, sync.getReturnCode(), "Error adding");
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("*** ASYNC WRITE COMPLETE ***");
            }
            // close ledger
            lh.close();

            // *** WRITING PART COMPLETED // READ PART BEGINS ***

            // open ledger
            lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Number of entries written: " + (lh.getLastAddConfirmed() + 1));
            }
            assertEquals(lh.getLastAddConfirmed(), (numEntriesToWrite - 1), "Verifying number of entries written");

            // read entries
            Enumeration<LedgerEntry> ls = lh.readEntries(0, numEntriesToWrite - 1);

            if (LOG.isDebugEnabled()) {
                LOG.debug("*** SYNC READ COMPLETE ***");
            }

            // at this point, Enumeration<LedgerEntry> ls is filled with the returned
            // values
            int i = 0;
            while (ls.hasMoreElements()) {
                byte[] origEntryBytes = entries.get(i++);
                byte[] retrEntryBytes = ls.nextElement().getEntry();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Original byte entry size: " + origEntryBytes.length);
                    LOG.debug("Saved byte entry size: " + retrEntryBytes.length);
                }

                String origEntry = new String(origEntryBytes, charset);
                String retrEntry = new String(retrEntryBytes, charset);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Original entry: " + origEntry);
                    LOG.debug("Retrieved entry: " + retrEntry);
                }

                assertEquals(origEntry, retrEntry, "Checking entry " + i + " for equality");
            }
            assertEquals(i, numEntriesToWrite, "Checking number of read entries");

            lh.close();
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }

    }

    @Test
    void readWriteSyncSingleClient() throws IOException {
        try {
            // Create a ledger
            lh = bkc.createLedger(digestType, ledgerPassword);
            // bkc.initMessageDigest("SHA1");
            ledgerId = lh.getId();
            LOG.info("Ledger ID: " + lh.getId());
            for (int i = 0; i < numEntriesToWrite; i++) {
                ByteBuffer entry = ByteBuffer.allocate(4);
                entry.putInt(rng.nextInt(maxInt));
                entry.position(0);
                entries.add(entry.array());
                lh.addEntry(entry.array());
            }
            lh.close();
            lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Number of entries written: " + lh.getLastAddConfirmed());
            }
            assertEquals(lh.getLastAddConfirmed(), (numEntriesToWrite - 1), "Verifying number of entries written");

            Enumeration<LedgerEntry> ls = lh.readEntries(0, numEntriesToWrite - 1);
            int i = 0;
            while (ls.hasMoreElements()) {
                ByteBuffer origbb = ByteBuffer.wrap(entries.get(i++));
                Integer origEntry = origbb.getInt();
                ByteBuffer result = ByteBuffer.wrap(ls.nextElement().getEntry());
                Integer retrEntry = result.getInt();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Length of result: " + result.capacity());
                    LOG.debug("Original entry: " + origEntry);
                    LOG.debug("Retrieved entry: " + retrEntry);
                }
                assertEquals(origEntry, retrEntry, "Checking entry " + i + " for equality");
            }
            lh.close();
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }

    @Test
    void readWriteZero() throws IOException {
        try {
            // Create a ledger
            lh = bkc.createLedger(digestType, ledgerPassword);
            // bkc.initMessageDigest("SHA1");
            ledgerId = lh.getId();
            LOG.info("Ledger ID: " + lh.getId());
            final CountDownLatch completeLatch = new CountDownLatch(numEntriesToWrite);
            final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);

            for (int i = 0; i < numEntriesToWrite; i++) {
                lh.asyncAddEntry(new byte[0], new AddCallback() {
                        public void addComplete(int rccb, LedgerHandle lh, long entryId, Object ctx) {
                            rc.compareAndSet(BKException.Code.OK, rccb);
                            completeLatch.countDown();
                        }
                    }, null);
            }
            completeLatch.await();
            if (rc.get() != BKException.Code.OK) {
                throw BKException.create(rc.get());
            }

            /*
             * Write a non-zero entry
             */
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            entries.add(entry.array());
            lh.addEntry(entry.array());

            lh.close();
            lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Number of entries written: " + lh.getLastAddConfirmed());
            }
            assertEquals(lh.getLastAddConfirmed(), numEntriesToWrite, "Verifying number of entries written");

            Enumeration<LedgerEntry> ls = lh.readEntries(0, numEntriesToWrite - 1);
            int i = 0;
            while (ls.hasMoreElements()) {
                ByteBuffer result = ByteBuffer.wrap(ls.nextElement().getEntry());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Length of result: " + result.capacity());
                }

                assertEquals(0, result.capacity(), "Checking if entry " + i + " has zero bytes");
            }
            lh.close();
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }

    @Test
    void multiLedger() throws IOException {
        try {
            // Create a ledger
            lh = bkc.createLedger(digestType, ledgerPassword);
            lh2 = bkc.createLedger(digestType, ledgerPassword);

            long ledgerId = lh.getId();
            long ledgerId2 = lh2.getId();

            final CountDownLatch completeLatch = new CountDownLatch(numEntriesToWrite * 2);
            final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);

            // bkc.initMessageDigest("SHA1");
            LOG.info("Ledger ID 1: " + lh.getId() + ", Ledger ID 2: " + lh2.getId());
            for (int i = 0; i < numEntriesToWrite; i++) {
                lh.asyncAddEntry(new byte[0], new AddCallback() {
                        public void addComplete(int rc2, LedgerHandle lh, long entryId, Object ctx) {
                            rc.compareAndSet(BKException.Code.OK, rc2);
                            completeLatch.countDown();
                        }
                    }, null);
                lh2.asyncAddEntry(new byte[0], new AddCallback() {
                        public void addComplete(int rc2, LedgerHandle lh, long entryId, Object ctx) {
                            rc.compareAndSet(BKException.Code.OK, rc2);
                            completeLatch.countDown();
                        }
                    }, null);
            }
            completeLatch.await();
            if (rc.get() != BKException.Code.OK) {
                throw BKException.create(rc.get());
            }

            lh.close();
            lh2.close();

            lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
            lh2 = bkc.openLedger(ledgerId2, digestType, ledgerPassword);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Number of entries written: " + lh.getLastAddConfirmed() + ", " + lh2.getLastAddConfirmed());
            }
            assertEquals(lh
                    .getLastAddConfirmed(), (numEntriesToWrite - 1), "Verifying number of entries written lh (" + lh.getLastAddConfirmed() + ")");
            assertEquals(lh2
                    .getLastAddConfirmed(), (numEntriesToWrite - 1), "Verifying number of entries written lh2 (" + lh2.getLastAddConfirmed() + ")");

            Enumeration<LedgerEntry> ls = lh.readEntries(0, numEntriesToWrite - 1);
            int i = 0;
            while (ls.hasMoreElements()) {
                ByteBuffer result = ByteBuffer.wrap(ls.nextElement().getEntry());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Length of result: " + result.capacity());
                }

                assertEquals(0, result.capacity(), "Checking if entry " + i + " has zero bytes");
            }
            lh.close();
            ls = lh2.readEntries(0, numEntriesToWrite - 1);
            i = 0;
            while (ls.hasMoreElements()) {
                ByteBuffer result = ByteBuffer.wrap(ls.nextElement().getEntry());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Length of result: " + result.capacity());
                }

                assertEquals(0, result.capacity(), "Checking if entry " + i + " has zero bytes");
            }
            lh2.close();
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }

    @Test
    void readWriteAsyncLength() throws IOException {
        SyncObj sync = new SyncObj();
        try {
            // Create a ledger
            lh = bkc.createLedger(digestType, ledgerPassword);
            // bkc.initMessageDigest("SHA1");
            ledgerId = lh.getId();
            LOG.info("Ledger ID: " + lh.getId());
            for (int i = 0; i < numEntriesToWrite; i++) {
                ByteBuffer entry = ByteBuffer.allocate(4);
                entry.putInt(rng.nextInt(maxInt));
                entry.position(0);

                entries.add(entry.array());
                entriesSize.add(entry.array().length);
                lh.asyncAddEntry(entry.array(), this, sync);
            }

            // wait for all entries to be acknowledged
            synchronized (sync) {
                while (sync.counter < numEntriesToWrite) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Entries counter = " + sync.counter);
                    }
                    sync.wait();
                }
                assertEquals(BKException.Code.OK, sync.getReturnCode(), "Error adding");
            }
            long length = numEntriesToWrite * 4;
            assertEquals(lh.getLength(), length, "Ledger length before closing: " + lh.getLength());

            if (LOG.isDebugEnabled()) {
                LOG.debug("*** WRITE COMPLETE ***");
            }
            // close ledger
            lh.close();

            // *** WRITING PART COMPLETE // READ PART BEGINS ***

            // open ledger
            lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
            assertEquals(lh.getLength(), length, "Ledger length after opening: " + lh.getLength());


            lh.close();
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }

    private long writeNEntriesLastWriteSync(LedgerHandle lh, int numToWrite) throws Exception {
        final CountDownLatch completeLatch = new CountDownLatch(numToWrite - 1);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);

        ByteBuffer entry = ByteBuffer.allocate(4);
        for (int i = 0; i < numToWrite - 1; i++) {
            entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries.add(entry.array());
            entriesSize.add(entry.array().length);
            lh.asyncAddEntry(entry.array(), new AddCallback() {
                    public void addComplete(int rccb, LedgerHandle lh, long entryId, Object ctx) {
                        rc.compareAndSet(BKException.Code.OK, rccb);
                        completeLatch.countDown();
                    }
                }, null);
        }
        completeLatch.await();
        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }

        entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);

        entries.add(entry.array());
        entriesSize.add(entry.array().length);
        lh.addEntry(entry.array());
        return lh.getLastAddConfirmed();
    }

    @Test
    void readFromOpenLedger() throws Exception {
        try {
            // Create a ledger
            lh = bkc.createLedger(digestType, ledgerPassword);
            // bkc.initMessageDigest("SHA1");
            ledgerId = lh.getId();
            LOG.info("Ledger ID: " + lh.getId());

            long lac = writeNEntriesLastWriteSync(lh, numEntriesToWrite);

            LedgerHandle lhOpen = bkc.openLedgerNoRecovery(ledgerId, digestType, ledgerPassword);
            // no recovery opened ledger 's last confirmed entry id is less than written
            // and it just can read until (i-1)
            long toRead = lac - 1;

            Enumeration<LedgerEntry> readEntry = lhOpen.readEntries(toRead, toRead);
            assertTrue(readEntry.hasMoreElements(), "Enumeration of ledger entries has no element");
            LedgerEntry e = readEntry.nextElement();
            assertEquals(toRead, e.getEntryId());
            assertArrayEquals(entries.get((int) toRead), e.getEntry());
            // should not written to a read only ledger
            try {
                ByteBuffer entry = ByteBuffer.allocate(4);
                entry.putInt(rng.nextInt(maxInt));
                entry.position(0);

                lhOpen.addEntry(entry.array());
                fail("Should have thrown an exception here");
            } catch (BKException.BKIllegalOpException bkioe) {
                // this is the correct response
            } catch (Exception ex) {
                LOG.error("Unexpected exception", ex);
                fail("Unexpected exception");
            }
            // close read only ledger should not change metadata
            lhOpen.close();

            lac = writeNEntriesLastWriteSync(lh, numEntriesToWrite);

            assertEquals(lac, (numEntriesToWrite * 2) - 1, "Last confirmed add: ");

            if (LOG.isDebugEnabled()) {
                LOG.debug("*** WRITE COMPLETE ***");
            }
            // close ledger
            lh.close();
            /*
             * Asynchronous call to read last confirmed entry
             */
            lh = bkc.createLedger(digestType, ledgerPassword);
            // bkc.initMessageDigest("SHA1");
            ledgerId = lh.getId();

            writeNEntriesLastWriteSync(lh, numEntriesToWrite);

            SyncObj sync = new SyncObj();
            lh.asyncReadLastConfirmed(this, sync);

            // Wait for for last confirmed
            synchronized (sync) {
                while (sync.lastConfirmed == -1) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Counter = " + sync.lastConfirmed);
                    }
                    sync.wait();
                }
                assertEquals(BKException.Code.OK, sync.getReturnCode(), "Error reading");
            }

            assertEquals(sync.lastConfirmed, (numEntriesToWrite - 2), "Last confirmed add");

            if (LOG.isDebugEnabled()) {
                LOG.debug("*** WRITE COMPLETE ***");
            }
            // close ledger
            lh.close();
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }

    @Test
    void readFromOpenLedgerOpenOnce() throws Exception {
        try {
            // Create a ledger
            lh = bkc.createLedger(digestType, ledgerPassword);
            // bkc.initMessageDigest("SHA1");
            ledgerId = lh.getId();
            LOG.info("Ledger ID: " + lh.getId());
            LedgerHandle lhOpen = bkc.openLedgerNoRecovery(ledgerId, digestType, ledgerPassword);
            writeNEntriesLastWriteSync(lh, numEntriesToWrite / 2);

            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            // no recovery opened ledger 's last confirmed entry id is
            // less than written
            // and it just can read until (i-1)
            int toRead = numEntriesToWrite / 2 - 2;

            long readLastConfirmed = lhOpen.readLastConfirmed();
            assertTrue(readLastConfirmed != 0);
            Enumeration<LedgerEntry> readEntry = lhOpen.readEntries(toRead, toRead);
            assertTrue(readEntry.hasMoreElements(), "Enumeration of ledger entries has no element");
            LedgerEntry e = readEntry.nextElement();
            assertEquals(toRead, e.getEntryId());
            assertArrayEquals(entries.get(toRead), e.getEntry());
            // should not written to a read only ledger
            try {
                lhOpen.addEntry(entry.array());
                fail("Should have thrown an exception here");
            } catch (BKException.BKIllegalOpException bkioe) {
                // this is the correct response
            } catch (Exception ex) {
                LOG.error("Unexpected exception", ex);
                fail("Unexpected exception");
            }
            writeNEntriesLastWriteSync(lh, numEntriesToWrite / 2);

            long last = lh.readLastConfirmed();
            assertEquals(last, (numEntriesToWrite - 2), "Last confirmed add: " + last);

            if (LOG.isDebugEnabled()) {
                LOG.debug("*** WRITE COMPLETE ***");
            }
            // close ledger
            lh.close();
            // close read only ledger should not change metadata
            lhOpen.close();
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }

    @Test
    void readFromOpenLedgerZeroAndOne() throws Exception {
        try {
            // Create a ledger
            lh = bkc.createLedger(digestType, ledgerPassword);
            // bkc.initMessageDigest("SHA1");
            ledgerId = lh.getId();
            LOG.info("Ledger ID: " + lh.getId());
            LedgerHandle lhOpen = bkc.openLedgerNoRecovery(ledgerId, digestType, ledgerPassword);

            /*
             * We haven't written anything, so it should be empty.
             */
            if (LOG.isDebugEnabled()) {
                LOG.debug("Checking that it is empty");
            }
            long readLastConfirmed = lhOpen.readLastConfirmed();
            assertEquals(LedgerHandle.INVALID_ENTRY_ID, readLastConfirmed, "Last confirmed has the wrong value");

            /*
             * Writing one entry.
             */
            if (LOG.isDebugEnabled()) {
                LOG.debug("Going to write one entry");
            }
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries.add(entry.array());
            entriesSize.add(entry.array().length);
            lh.addEntry(entry.array());

            /*
             * The hint should still indicate that there is no confirmed
             * add.
             */
            if (LOG.isDebugEnabled()) {
                LOG.debug("Checking that it is still empty even after writing one entry");
            }
            readLastConfirmed = lhOpen.readLastConfirmed();
            assertEquals(LedgerHandle.INVALID_ENTRY_ID, readLastConfirmed);

            /*
             * Adding one more, and this time we should expect to
             * see one entry.
             */
            entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries.add(entry.array());
            entriesSize.add(entry.array().length);
            lh.addEntry(entry.array());

            LOG.info("Checking that it has an entry");
            readLastConfirmed = lhOpen.readLastConfirmed();
            assertEquals(0L, readLastConfirmed);

            // close ledger
            lh.close();
            // close read only ledger should not change metadata
            lhOpen.close();
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }

    @Test
    void writeUsingReadOnlyHandle() throws Exception {
        // Create a ledger
        lh = bkc.createLedger(digestType, ledgerPassword);
        ledgerId = lh.getId();
        LOG.info("Ledger ID: " + lh.getId());

        long lac = writeNEntriesLastWriteSync(lh, numEntriesToWrite);
        LedgerHandle lhOpen = bkc.openLedgerNoRecovery(ledgerId, digestType, ledgerPassword);

        // addEntry on ReadOnlyHandle should fail
        CountDownLatch latch = new CountDownLatch(1);
        final int[] rcArray = { 0 };
        lhOpen.asyncAddEntry("".getBytes(), new AddCallback() {
            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                CountDownLatch latch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                latch.countDown();
            }
        }, latch);
        latch.await();
        if (rcArray[0] != BKException.Code.IllegalOpException) {
            fail("Test1 - asyncAddOperation is supposed to be failed, but it got following rc - "
                    + KeeperException.Code.get(rcArray[0]));
        }

        // addEntry on ReadOnlyHandle should fail
        latch = new CountDownLatch(1);
        rcArray[0] = 0;
        lhOpen.asyncAddEntry("".getBytes(), 0, 0, new AddCallback() {
            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                CountDownLatch latch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                latch.countDown();
            }
        }, latch);
        latch.await();
        if (rcArray[0] != BKException.Code.IllegalOpException) {
            fail(
                    "Test2 - asyncAddOperation is supposed to fail with IllegalOpException, but it got following rc - "
                            + KeeperException.Code.get(rcArray[0]));
        }

        // close readonlyhandle
        latch = new CountDownLatch(1);
        rcArray[0] = 0;
        lhOpen.asyncClose(new CloseCallback() {
            @Override
            public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
                CountDownLatch latch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                latch.countDown();
            }
        }, latch);
        latch.await();
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            fail("Test3 - asyncClose failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }

        // close of readonlyhandle should not affect the writehandle
        writeNEntriesLastWriteSync(lh, 5);
        lh.close();
    }

    @Test
    void ledgerHandle() throws Exception {
        // Create a ledger
        lh = bkc.createLedger(digestType, ledgerPassword);
        ledgerId = lh.getId();
        LOG.info("Ledger ID: " + lh.getId());

        long lac = writeNEntriesLastWriteSync(lh, 5);

        // doing addEntry with entryid using regular Ledgerhandle should fail
        CountDownLatch latch = new CountDownLatch(1);
        final int[] rcArray = { 0 };
        lh.asyncAddEntry(lac + 1, "".getBytes(), new AddCallback() {
            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                CountDownLatch latch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                latch.countDown();
            }
        }, latch);
        latch.await();
        if (rcArray[0] != BKException.Code.IllegalOpException) {
            fail(
                    "Test1 - addEntry with EntryID is expected to fail with IllegalOpException, "
                    + "but it got following rc - " + KeeperException.Code.get(rcArray[0]));
        }

        // doing addEntry with entryid using regular Ledgerhandle should fail
        latch = new CountDownLatch(1);
        rcArray[0] = 0;
        lh.asyncAddEntry(lac + 1, "".getBytes(), 0, 0, new AddCallback() {
            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                CountDownLatch latch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                latch.countDown();
            }
        }, latch);
        latch.await();
        if (rcArray[0] != BKException.Code.IllegalOpException) {
            fail(
                    "Test2 - addEntry with EntryID is expected to fail with IllegalOpException,"
                    + "but it got following rc - " + KeeperException.Code.get(rcArray[0]));
        }

        // doing addEntry with entryid using regular Ledgerhandle should fail
        try {
            lh.addEntry(lac + 1, "".getBytes());
            fail("Test3 - addEntry with EntryID is expected to fail");
        } catch (BKIllegalOpException E) {
        }

        // doing addEntry with entryid using regular Ledgerhandle should fail
        try {
            lh.addEntry(lac + 1, "".getBytes(), 0, 0);
            fail("Test4 - addEntry with EntryID is expected to fail");
        } catch (BKIllegalOpException E) {
        }

        lh.close();
    }

    @Test
    void lastConfirmedAdd() throws Exception {
        try {
            // Create a ledger
            lh = bkc.createLedger(digestType, ledgerPassword);
            ledgerId = lh.getId();
            LOG.info("Ledger ID: " + lh.getId());

            writeNEntriesLastWriteSync(lh, numEntriesToWrite);
            long last = lh.readLastConfirmed();
            assertEquals(last, (numEntriesToWrite - 2), "Last confirmed add: " + last);

            if (LOG.isDebugEnabled()) {
                LOG.debug("*** WRITE COMPLETE ***");
            }
            // close ledger
            lh.close();
            /*
             * Asynchronous call to read last confirmed entry
             */
            lh = bkc.createLedger(digestType, ledgerPassword);
            // bkc.initMessageDigest("SHA1");
            ledgerId = lh.getId();
            LOG.info("Ledger ID: " + lh.getId());
            writeNEntriesLastWriteSync(lh, numEntriesToWrite);

            SyncObj sync = new SyncObj();
            lh.asyncReadLastConfirmed(this, sync);

            // Wait for for last confirmed
            synchronized (sync) {
                while (sync.lastConfirmed == LedgerHandle.INVALID_ENTRY_ID) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Counter = " + sync.lastConfirmed);
                    }
                    sync.wait();
                }
                assertEquals(BKException.Code.OK, sync.getReturnCode(), "Error reading");
            }

            assertEquals(sync.lastConfirmed, (numEntriesToWrite - 2), "Last confirmed add: " + sync.lastConfirmed);

            if (LOG.isDebugEnabled()) {
                LOG.debug("*** WRITE COMPLETE ***");
            }
            // close ledger
            lh.close();
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }

    @Test
    void readLastConfirmed() throws Exception {
        // Create a ledger and add entries
        lh = bkc.createLedger(digestType, ledgerPassword);
        // bkc.initMessageDigest("SHA1");
        ledgerId = lh.getId();
        LOG.info("Ledger ID: " + lh.getId());
        long previousLAC = writeNEntriesLastWriteSync(lh, 5);

        // add more entries after opening ReadonlyLedgerHandle
        LedgerHandle lhOpen = bkc.openLedgerNoRecovery(ledgerId, digestType, ledgerPassword);
        long currentLAC = writeNEntriesLastWriteSync(lh, 5);

        // get LAC instance variable of ReadHandle and verify if it is equal to (previousLAC - 1)
        long readLAC = lhOpen.getLastAddConfirmed();
        assertEquals((previousLAC - 1), readLAC, "Test1 - For ReadHandle LAC");

        // close the write LedgerHandle and sleep for 500 msec to make sure all close watchers are called
        lh.close();
        Thread.sleep(500);

        // now call asyncReadLastConfirmed and verify if it is equal to currentLAC
        CountDownLatch latch = new CountDownLatch(1);
        final int[] rcArray = { 0 };
        final long[] lastConfirmedArray = { 0 };
        lhOpen.asyncReadLastConfirmed(new ReadLastConfirmedCallback() {
            @Override
            public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
                CountDownLatch latch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                lastConfirmedArray[0] = lastConfirmed;
                latch.countDown();
            }
        }, latch);
        latch.await();
        assertEquals(KeeperException.Code.OK.intValue(), rcArray[0], "Test3 - asyncReadLastConfirmed response");
        assertEquals(currentLAC, lastConfirmedArray[0], "Test3 - ReadLAC");

        // similarly try calling asyncTryReadLastConfirmed and verify if it is equal to currentLAC
        latch = new CountDownLatch(1);
        rcArray[0] = 0;
        lastConfirmedArray[0] = 0;
        lhOpen.asyncTryReadLastConfirmed(new ReadLastConfirmedCallback() {
            @Override
            public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
                CountDownLatch latch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                lastConfirmedArray[0] = lastConfirmed;
                latch.countDown();
            }
        }, latch);
        latch.await();
        assertEquals(KeeperException.Code.OK.intValue(),
                rcArray[0],
                "Test4 - asyncTryReadLastConfirmed response");
        assertEquals(currentLAC, lastConfirmedArray[0], "Test4 - ReadLAC");

        // similarly try calling tryReadLastConfirmed and verify if it is equal to currentLAC
        long tryReadLAC = lhOpen.tryReadLastConfirmed();
        assertEquals(currentLAC, tryReadLAC, "Test5 - ReadLAC");
    }

    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        SyncObj sync = (SyncObj) ctx;
        sync.setReturnCode(rc);
        synchronized (sync) {
            sync.counter++;
            sync.notify();
        }
    }

    @Override
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
        SyncObj sync = (SyncObj) ctx;
        sync.setLedgerEntries(seq);
        sync.setReturnCode(rc);
        synchronized (sync) {
            sync.value = true;
            sync.notify();
        }
    }

    @Override
    public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
        SyncObj sync = (SyncObj) ctx;
        sync.setReturnCode(rc);
        synchronized (sync) {
            sync.lastConfirmed = lastConfirmed;
            sync.notify();
        }
    }

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        rng = new Random(System.currentTimeMillis()); // Initialize the Random
        // Number Generator
        entries = new ArrayList<byte[]>(); // initialize the entries list
        entriesSize = new ArrayList<Integer>();
    }

    /* Clean up a directory recursively */
    protected boolean cleanUpDir(File dir) {
        if (dir.isDirectory()) {
            LOG.info("Cleaning up " + dir.getName());
            String[] children = dir.list();
            for (String string : children) {
                boolean success = cleanUpDir(new File(dir, string));
                if (!success) {
                    return false;
                }
            }
        }
        // The directory is now empty so delete it
        return dir.delete();
    }

    /**
     * Used for testing purposes, void.
     */
    class EmptyWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
        }
    }

}
