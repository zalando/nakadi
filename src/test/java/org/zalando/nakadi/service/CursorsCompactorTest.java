package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.view.SubscriptionCursor;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CursorsCompactorTest {

    private CursorCompactor cursorCompactor;

    @Before
    public void setUp() throws Exception {
        final TopicRepository topicRepository =  mock(TopicRepository.class);
        when(topicRepository.compareOffsets(any(), any())).thenAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(final InvocationOnMock invocation) throws Throwable {
                final NakadiCursor nc1 = (NakadiCursor) invocation.getArguments()[0];
                final NakadiCursor nc2 = (NakadiCursor) invocation.getArguments()[1];
                return Long.compare(Long.parseLong(nc1.getOffset()), Long.parseLong(nc2.getOffset()));
            }
        });
        cursorCompactor = new CursorCompactor(topicRepository);
    }

    @Test
    public void blah() {
        final ImmutableList<SubscriptionCursor> cursors = ImmutableList.of(
                new SubscriptionCursor("0", "000002131", "et", ""),
                new SubscriptionCursor("0", "BEGIN", "et", ""),
                new SubscriptionCursor("0", "009877", "et", ""),
                new SubscriptionCursor("0", "7", "et", ""),
                new SubscriptionCursor("1", "0000123", "et", ""),
                new SubscriptionCursor("1", "7786", "et", ""),
                new SubscriptionCursor("1", "11", "et", ""),
                new SubscriptionCursor("1", "002221", "et", ""),
                new SubscriptionCursor("0", "72190", "et", ""),
                new SubscriptionCursor("0", "99", "et", "")
        );

        final List<SubscriptionCursor> compactCursors = cursorCompactor.compactCursors(cursors);

        assertThat(compactCursors, equalTo(ImmutableList.of()));
        System.out.println(compactCursors);
    }
}