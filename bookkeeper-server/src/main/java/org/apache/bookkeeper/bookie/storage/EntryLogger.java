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
package org.apache.bookkeeper.bookie.storage;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Collection;
import org.apache.bookkeeper.bookie.AbstractLogCompactor;
import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.EntryLogMetadata;


/**
 * Entry logger. Sequentially writes entries for a large number of ledgers to
 * a small number of log files, to avoid many random writes.
 * When an entry is added, a location is returned, which consists of the ID of the
 * log into which the entry was added, and the offset of that entry within the log.
 * The location is a long, with 32 bits each for the log ID and the offset. This
 * naturally limits the offset and thus the size of the log to Integer.MAX_VALUE.
 *
 * 日志记录器。将大量账本的条目按顺序写入少量日志文件，以避免大量随机写入。当添加一个条目时，会返回一个位置，其中包含添加条目的日志的ID，
 * 以及该日志中的条目偏移量。位置是一个 long 型变量，其中有 32 位分别用于日志 ID 和偏移量。这自然地限制了偏移量和因此日志的大小为 Integer.MAX_VALUE。
 */
//entryLogger代表着存储实际数据的组件抽象
public interface EntryLogger extends AutoCloseable {
    long UNASSIGNED_LEDGERID = -1L;
    // log file suffix
    String LOG_FILE_SUFFIX = ".log";

    /**
     * Add an entry for ledger ```ledgerId``` to the entrylog.
     * @param ledgerId the ledger for which the entry is being added
     * @param buf the contents of the entry (this method does not take ownership of the refcount)
     * @return the location in the entry log of the added entry
     */
    long addEntry(long ledgerId, ByteBuf buf) throws IOException;

    /**
     * Read an entry from an entrylog location.
     * @param entryLocation the location from which to read the entry
     * @return the entry
     */
    ByteBuf readEntry(long entryLocation)
            throws IOException, NoEntryException;
    /**
     * Read an entry from an entrylog location, and verify that is matches the
     * expected ledger and entry ID.
     * @param ledgerId the ledgerID to match
     * @param entryId the entryID to match
     * @param entryLocation the location from which to read the entry
     * @return the entry
     */
    ByteBuf readEntry(long ledgerId, long entryId, long entryLocation)
            throws IOException, NoEntryException;

    /**
     * Flush any outstanding writes to disk.
     */
    void flush() throws IOException;

    @Override
    void close() throws IOException;

    /**
     * Create a new entrylog into which compacted entries can be added.
     * There is a 1-1 mapping between logs that are being compacted
     * and the log the compacted entries are written to.
     */
    CompactionEntryLog newCompactionLog(long logToCompact) throws IOException;

    /**
     * Return a collection of all the compaction entry logs which have been
     * compacted, but have not been cleaned up.
     */
    Collection<CompactionEntryLog> incompleteCompactionLogs();

    /**
     * Get the log ids for the set of logs which have been completely flushed to
     * disk.
     * Only log ids in this set are considered for either compaction or garbage
     * collection.
     */
    Collection<Long> getFlushedLogIds();

    /**
     * Scan the given entrylog, returning all entries contained therein.
     */
    void scanEntryLog(long entryLogId, EntryLogScanner scanner) throws IOException;

    /**
     * Retrieve metadata for the given entrylog ID.
     * The metadata contains the size of the log, the size of the data in the log which is still
     * active, and a list of all the ledgers contained in the log and the size of the data stored
     * for each ledger.
     */
    default EntryLogMetadata getEntryLogMetadata(long entryLogId) throws IOException {
        return getEntryLogMetadata(entryLogId, null);
    }

    /**
     * Retrieve metadata for the given entrylog ID.
     * The metadata contains the size of the log, the size of the data in the log which is still
     * active, and a list of all the ledgers contained in the log and the size of the data stored
     * for each ledger.
     */
    EntryLogMetadata getEntryLogMetadata(long entryLogId, AbstractLogCompactor.Throttler throttler) throws IOException;

    /**
     * Check whether an entrylog with the given ID exists.
     */
    boolean logExists(long logId);

    /**
     * Delete the entrylog with the given ID.
     * @return false if the entrylog doesn't exist.
     */
    boolean removeEntryLog(long entryLogId);
}
