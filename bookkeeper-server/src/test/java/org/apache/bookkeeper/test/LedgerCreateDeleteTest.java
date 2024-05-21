package org.apache.bookkeeper.test;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test Create/Delete ledgers.
 */
public class LedgerCreateDeleteTest extends BookKeeperClusterTestCase {

    public LedgerCreateDeleteTest() {
        super(1);
    }

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        baseConf.setOpenFileLimit(1);
        super.setUp();
    }

    @Test
    void createDeleteLedgers() throws Exception {
        int numLedgers = 3;
        ArrayList<Long> ledgers = new ArrayList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(1, 1, DigestType.CRC32, "bk is cool".getBytes());
            for (int j = 0; j < 5; j++) {
                lh.addEntry("just test".getBytes());
            }
            ledgers.add(lh.getId());
            lh.close();
        }
        for (long ledgerId : ledgers) {
            bkc.deleteLedger(ledgerId);
        }
        ledgers.clear();
        Thread.sleep(baseConf.getGcWaitTime() * 2);
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(1, 1, DigestType.CRC32, "bk is cool".getBytes());
            for (int j = 0; j < 5; j++) {
                lh.addEntry("just test".getBytes());
            }
            ledgers.add(lh.getId());
            lh.close();
        }
    }

    @Test
    void createLedgerWithBKNotEnoughBookiesException() throws Exception {
        try {
            bkc.createLedger(2, 2, DigestType.CRC32, "bk is cool".getBytes());
            fail("Should be able to throw BKNotEnoughBookiesException");
        } catch (BKException.BKNotEnoughBookiesException bkn) {
            // expected
        }
    }

    @Test
    void createLedgerWithZKException() throws Exception {
        stopZKCluster();
        try {
            bkc.createLedger(1, 1, DigestType.CRC32, "bk is cool".getBytes());
            fail("Should be able to throw ZKException");
        } catch (BKException.ZKException zke) {
            // expected
        }
    }

}
