package org.zalando.nakadi.service.subscription.autocommit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.List;
import java.util.stream.LongStream;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AutocommitSupportTest {

    @Mock
    private ZkSubscriptionClient zkClientMock;
    @Mock
    private CursorOperationsService cursorOperationsService;
    private AutocommitSupport autocommitSupport;
    private EventTypePartition etp1;
    private NakadiCursor[] etp1Cursors;
    private EventTypePartition etp2;
    private NakadiCursor[] etp2Cursors;

    @Captor
    private ArgumentCaptor<List<SubscriptionCursorWithoutToken>> commitOffsetsCaptor;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        autocommitSupport = new AutocommitSupport(cursorOperationsService, zkClientMock);

        etp1 = new EventTypePartition("t", "p1");
        etp1Cursors = mockCursors(etp1, LongStream.range(0, 10).toArray());
        autocommitSupport.addPartition(etp1Cursors[0]);

        etp2 = new EventTypePartition("t", "p2");
        etp2Cursors = mockCursors(etp2, LongStream.range(0, 10).toArray());
        autocommitSupport.addPartition(etp2Cursors[0]);
    }

    @Test
    public void testNoException() {
        final EventTypePartition nonRegisteredEtp = new EventTypePartition("t", "p0");
        final NakadiCursor[] cursors = mockCursors(nonRegisteredEtp, 0L);
        autocommitSupport.addSkippedEvent(cursors[0]);
    }

    @Test
    public void testAutocommitIssuedFor1Partition() {
        autocommitSupport.addSkippedEvent(etp1Cursors[1]);
        autocommitSupport.autocommit();
        verify(zkClientMock, times(1)).commitOffsets(commitOffsetsCaptor.capture());

        final List<SubscriptionCursorWithoutToken> actuallyCommitted = commitOffsetsCaptor.getValue();
        Assert.assertEquals(1, actuallyCommitted.size());
        Assert.assertEquals(new SubscriptionCursorWithoutToken("t", "p1", "1"), actuallyCommitted.get(0));
        autocommitSupport.onCommit(etp1Cursors[1]);
        autocommitSupport.autocommit();
        // Verify that second call was not issued
        verify(zkClientMock, times(1)).commitOffsets(commitOffsetsCaptor.capture());
    }

    @Test
    public void testAutocommitIssuedFor2Partition() {
        autocommitSupport.addSkippedEvent(etp1Cursors[1]);
        autocommitSupport.addSkippedEvent(etp2Cursors[1]);
        autocommitSupport.addSkippedEvent(etp2Cursors[2]);
        autocommitSupport.autocommit();
        verify(zkClientMock, times(1)).commitOffsets(commitOffsetsCaptor.capture());

        final List<SubscriptionCursorWithoutToken> actuallyCommitted = commitOffsetsCaptor.getValue();
        Assert.assertEquals(2, actuallyCommitted.size());
        if (actuallyCommitted.get(0).getEventTypePartition().getPartition().equals("p1")) {
            Assert.assertEquals(new SubscriptionCursorWithoutToken("t", "p1", "1"), actuallyCommitted.get(0));
            Assert.assertEquals(new SubscriptionCursorWithoutToken("t", "p2", "2"), actuallyCommitted.get(1));
        } else {
            Assert.assertEquals(new SubscriptionCursorWithoutToken("t", "p1", "1"), actuallyCommitted.get(1));
            Assert.assertEquals(new SubscriptionCursorWithoutToken("t", "p2", "2"), actuallyCommitted.get(2));
        }
        autocommitSupport.onCommit(etp1Cursors[1]);
        autocommitSupport.onCommit(etp2Cursors[2]);
        autocommitSupport.autocommit();
        // Verify that second call was not issued
        verify(zkClientMock, times(1)).commitOffsets(commitOffsetsCaptor.capture());
    }

    private NakadiCursor[] mockCursors(final EventTypePartition etp, final long... positions) {
        return PartitionSkippedCursorsOperatorTest.mockCursors(cursorOperationsService, etp, positions);
    }

}