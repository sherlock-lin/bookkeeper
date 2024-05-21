package org.apache.bookkeeper.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test tests timeout of GetBookieInfo request.
 *
 */
public class TestGetBookieInfoTimeout extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TestGetBookieInfoTimeout.class);
    DigestType digestType;
    public EventLoopGroup eventLoopGroup;
    public OrderedExecutor executor;
    private ScheduledExecutorService scheduler;

    public TestGetBookieInfoTimeout() {
        super(5);
        this.digestType = DigestType.CRC32C;
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        eventLoopGroup = new NioEventLoopGroup();

        executor = OrderedExecutor.newBuilder()
                .name("BKClientOrderedSafeExecutor")
                .numThreads(2)
                .build();
        scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler"));
    }

    @AfterEach
    public void tearDown() throws Exception {
        scheduler.shutdown();
        eventLoopGroup.shutdownGracefully();
        executor.shutdown();
    }

    @Test
    void getBookieInfoTimeout() throws Exception {

        // connect to the bookies and create a ledger
        LedgerHandle writelh = bkc.createLedger(3, 3, digestType, "testPasswd".getBytes());
        String tmp = "Foobar";
        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        // set timeout for getBookieInfo to be 2 secs and cause one of the bookies to go to sleep for 3X that time
        ClientConfiguration cConf = new ClientConfiguration();
        cConf.setGetBookieInfoTimeout(2);
        cConf.setReadEntryTimeout(100000); // by default we are using readEntryTimeout for timeouts

        final BookieId bookieToSleep = writelh.getLedgerMetadata().getEnsembleAt(0).get(0);
        int sleeptime = cConf.getBookieInfoTimeout() * 3;
        CountDownLatch latch = sleepBookie(bookieToSleep, sleeptime);
        latch.await();

        // try to get bookie info from the sleeping bookie. It should fail with timeout error
        BookieClient bc = new BookieClientImpl(cConf, eventLoopGroup, UnpooledByteBufAllocator.DEFAULT, executor,
                scheduler, NullStatsLogger.INSTANCE, bkc.getBookieAddressResolver());
        long flags = BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE
                | BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE;

        class CallbackObj {
            int rc;
            long requested;
            @SuppressWarnings("unused")
            long freeDiskSpace, totalDiskCapacity;
            CountDownLatch latch = new CountDownLatch(1);
            CallbackObj(long requested) {
                this.requested = requested;
                this.rc = 0;
                this.freeDiskSpace = 0L;
                this.totalDiskCapacity = 0L;
            }
        }
        CallbackObj obj = new CallbackObj(flags);
        bc.getBookieInfo(bookieToSleep, flags, new GetBookieInfoCallback() {
            @Override
            public void getBookieInfoComplete(int rc, BookieInfo bInfo, Object ctx) {
                CallbackObj obj = (CallbackObj) ctx;
                obj.rc = rc;
                if (rc == Code.OK) {
                    if ((obj.requested & BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE) != 0) {
                        obj.freeDiskSpace = bInfo.getFreeDiskSpace();
                    }
                    if ((obj.requested & BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE)
                            != 0) {
                        obj.totalDiskCapacity = bInfo.getTotalDiskSpace();
                    }
                }
                obj.latch.countDown();
            }

        }, obj);
        obj.latch.await();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Return code: " + obj.rc);
        }
        assertEquals(Code.TimeoutException, obj.rc, "GetBookieInfo failed with unexpected error code: " + obj.rc);
    }

    @Test
    void getBookieInfoWithAllStoppedBookies() throws Exception {
        Map<BookieId, BookieInfo> bookieInfo = bkc.getBookieInfo();
        assertEquals(5, bookieInfo.size());
        stopAllBookies(false);
        bookieInfo = bkc.getBookieInfo();
        assertEquals(0, bookieInfo.size());
    }
}
