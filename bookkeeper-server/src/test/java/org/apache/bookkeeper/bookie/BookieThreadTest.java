/*
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
package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Testing bookie thread cases.
 */
class BookieThreadTest {

    private CountDownLatch runningLatch = new CountDownLatch(1);

    /**
     * A BookieThread implementation.
     */
    public class MyThread extends BookieThread {

        public MyThread(String threadName) {
            super(threadName);
        }

        public void run() {
            throw new Error();
        }

        @Override
        protected void handleException(Thread t, Throwable e) {
            assertEquals(this, t, "Unknown thread!");
            runningLatch.countDown();
        }
    }

    /**
     * A critical thread implementation.
     */
    public class MyCriticalThread extends BookieCriticalThread {

        public MyCriticalThread(String threadName) {
            super(threadName);
        }

        public void run() {
            throw new Error();
        }

        @Override
        protected void handleException(Thread t, Throwable e) {
            assertEquals(this, t, "Unknown thread!");
            runningLatch.countDown();
        }
    }

    /**
     * Test verifies uncaught exception handling of BookieThread.
     */
    @Test
    void uncaughtException() throws Exception {
        MyThread myThread = new MyThread("Test-Thread");
        myThread.start();
        assertTrue(runningLatch.await(10000, TimeUnit.MILLISECONDS),
                "Uncaught exception is not properly handled.");

        runningLatch = new CountDownLatch(1);
        MyCriticalThread myCriticalThread = new MyCriticalThread(
                "Test-Critical-Thread");
        myCriticalThread.start();
        assertTrue(runningLatch.await(10000, TimeUnit.MILLISECONDS),
                "Uncaught exception is not properly handled.");
    }
}
