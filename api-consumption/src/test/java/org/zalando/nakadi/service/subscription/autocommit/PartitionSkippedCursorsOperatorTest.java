package org.zalando.nakadi.service.subscription.autocommit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.service.CursorOperationsService;

import java.util.stream.LongStream;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionSkippedCursorsOperatorTest {

    private CursorOperationsService cursorOperationsService;

    @Before
    public void before() {
        cursorOperationsService = mock(CursorOperationsService.class);
    }

    @Test
    public void testNoSuggestionOnAbsenceOfSkippedEvents() {
        final NakadiCursor committed = mock(NakadiCursor.class);
        final PartitionSkippedCursorsOperator state =
                new PartitionSkippedCursorsOperator(cursorOperationsService, committed);

        Assert.assertNull(state.getAutoCommitSuggestion());
    }

    @Test
    public void testAutocommitSuggestionIsNullAfterCommit() {
        final NakadiCursor[] cursors = mockCursors(0, 1, 2);

        final PartitionSkippedCursorsOperator state =
                new PartitionSkippedCursorsOperator(cursorOperationsService, cursors[0]);
        state.addSkippedEvent(cursors[1]);
        state.onCommit(cursors[2]);

        Assert.assertNull(state.getAutoCommitSuggestion());
    }

    @Test
    public void testAutocommitSuggestionAbsentWhenNotRead() {
        final NakadiCursor[] cursors = mockCursors(0, 1, 2);
        final PartitionSkippedCursorsOperator state =
                new PartitionSkippedCursorsOperator(cursorOperationsService, cursors[0]);

        // skipping second event, but first is not committed yet
        state.addSkippedEvent(cursors[2]);

        Assert.assertNull(state.getAutoCommitSuggestion());
    }

    @Test
    public void testSimpleAutocommitSuggestion() {
        final NakadiCursor[] cursors = mockCursors(0, 1);
        final PartitionSkippedCursorsOperator state =
                new PartitionSkippedCursorsOperator(cursorOperationsService, cursors[0]);
        state.addSkippedEvent(cursors[1]);

        Assert.assertEquals(cursors[1], state.getAutoCommitSuggestion());
    }

    @Test
    public void test2InARow() {
        final NakadiCursor[] cursors = mockCursors(0, 1, 2);

        final PartitionSkippedCursorsOperator state =
                new PartitionSkippedCursorsOperator(cursorOperationsService, cursors[0]);
        state.addSkippedEvent(cursors[1]);
        state.addSkippedEvent(cursors[2]);

        Assert.assertEquals(cursors[2], state.getAutoCommitSuggestion());
    }

    @Test
    public void testZebra() {
        final NakadiCursor[] cursors = mockCursors(LongStream.range(0, 10).toArray());
        final PartitionSkippedCursorsOperator state =
                new PartitionSkippedCursorsOperator(cursorOperationsService, cursors[0]);

        state.addSkippedEvent(cursors[2]);
        state.addSkippedEvent(cursors[4]);
        state.addSkippedEvent(cursors[6]);
        state.addSkippedEvent(cursors[8]);

        Assert.assertNull(state.getAutoCommitSuggestion());
        state.onCommit(cursors[1]);
        Assert.assertEquals(cursors[2], state.getAutoCommitSuggestion());
        state.onCommit(state.getAutoCommitSuggestion());
        Assert.assertNull(state.getAutoCommitSuggestion());

        // Lets's say we commit further
        state.onCommit(cursors[7]);
        Assert.assertEquals(cursors[8], state.getAutoCommitSuggestion());
        state.onCommit(cursors[9]);
        Assert.assertNull(state.getAutoCommitSuggestion());
    }

    @Test
    public void testZebraWithWideStripes() {
        final NakadiCursor[] cursors = mockCursors(LongStream.range(0, 20).toArray());
        final PartitionSkippedCursorsOperator state =
                new PartitionSkippedCursorsOperator(cursorOperationsService, cursors[0]);

        state.addSkippedEvent(cursors[1]);
        state.addSkippedEvent(cursors[2]);

        state.addSkippedEvent(cursors[5]);
        state.addSkippedEvent(cursors[6]);

        state.addSkippedEvent(cursors[9]);
        state.addSkippedEvent(cursors[10]);

        state.addSkippedEvent(cursors[13]);
        state.addSkippedEvent(cursors[14]);

        state.addSkippedEvent(cursors[17]);
        state.addSkippedEvent(cursors[18]);

        Assert.assertEquals(cursors[2], state.getAutoCommitSuggestion());

        // 1. skip exactly what was suggested
        state.onCommit(state.getAutoCommitSuggestion());
        Assert.assertNull(state.getAutoCommitSuggestion());

        // 2. commit not enough
        state.onCommit(cursors[3]);
        Assert.assertNull(state.getAutoCommitSuggestion());

        // 3. commit enough
        state.onCommit(cursors[4]);
        Assert.assertEquals(cursors[6], state.getAutoCommitSuggestion());

        // 4. Commit in the middle of interval
        state.onCommit(cursors[9]);
        Assert.assertEquals(cursors[10], state.getAutoCommitSuggestion());

        // 5. Commit skipping stripe
        state.onCommit(cursors[16]);
        Assert.assertEquals(cursors[18], state.getAutoCommitSuggestion());
    }

    static NakadiCursor[] mockCursors(
            final CursorOperationsService service, final EventTypePartition etp, final long[] positions) {
        final NakadiCursor[] result = new NakadiCursor[positions.length];
        for (int i = 0; i < positions.length; ++i) {
            result[i] = mock(NakadiCursor.class);
            when(result[i].getEventType()).thenReturn(etp.getEventType());
            when(result[i].getPartition()).thenReturn(etp.getPartition());
            when(result[i].getEventTypePartition()).thenReturn(etp);
            when(result[i].getOffset()).thenReturn(String.valueOf(positions[i]));
        }
        for (int i = 0; i < result.length; i++) {
            for (int j = 0; j < result.length; j++) {
                when(service.calculateDistance(eq(result[i]), eq(result[j])))
                        .thenReturn(positions[j] - positions[i]);
            }
        }
        return result;
    }

    /**
     * Mocks cursor list with positions according to positions.
     *
     * @param positions list of nakadi cursors
     * @return cursors with positions specified in parameters
     */
    private NakadiCursor[] mockCursors(final long... positions) {
        return mockCursors(cursorOperationsService, new EventTypePartition("t", "0"), positions);
    }

}