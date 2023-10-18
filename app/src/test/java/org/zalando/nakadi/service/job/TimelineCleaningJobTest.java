package org.zalando.nakadi.service.job;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.Iterator;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class TimelineCleaningJobTest {

    private final TimelineCleanupJob timelineCleanupJob;
    private final TimelineService timelineService;
    private final EventTypeCache eventTypeCache;
    private final TimelineDbRepository timelineDbRepository;

    public TimelineCleaningJobTest() {
        timelineService = mock(TimelineService.class);
        eventTypeCache = mock(EventTypeCache.class);
        timelineDbRepository = mock(TimelineDbRepository.class);

        final JobWrapperFactory jobWrapperFactory = mock(JobWrapperFactory.class);
        final ExclusiveJobWrapper jobWrapper = DummyJobWrapper.create();
        when(jobWrapperFactory.createExclusiveJobWrapper(any(), anyLong())).thenReturn(jobWrapper);

        final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
        when(featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS))
                .thenReturn(false);

        timelineCleanupJob = new TimelineCleanupJob(eventTypeCache, timelineDbRepository, timelineService,
                featureToggleService, jobWrapperFactory, 0, 0L);
    }

    @Test
    public void whenCleanupTimelinesThenOk() throws Exception {
        final Timeline t1 = createTimeline("et1", "topic1");
        final Timeline t2 = createTimeline("et2", "topic2");

        final ImmutableList<Timeline> expiredTimelines = ImmutableList.of(t1, t2);
        when(timelineDbRepository.getExpiredTimelines()).thenReturn(expiredTimelines);

        final TopicRepository topicRepository = mock(TopicRepository.class);
        when(timelineService.getTopicRepository(argThat(isOneOf(t1, t2)))).thenReturn(topicRepository);

        timelineCleanupJob.cleanupTimelines();

        final ArgumentCaptor<Timeline> timelineCaptor = ArgumentCaptor.forClass(Timeline.class);
        verify(timelineDbRepository, times(2)).updateTimelime(timelineCaptor.capture());
        final List<Timeline> updatedTimelines = timelineCaptor.getAllValues();
        final Iterator<Timeline> updatedTimelinesIterator = updatedTimelines.iterator();

        for (final Timeline timeline : expiredTimelines) {
            verify(topicRepository).deleteTopic(timeline.getTopic());
            verify(eventTypeCache).invalidate(timeline.getEventType());

            final Timeline updatedTimeline = updatedTimelinesIterator.next();
            assertThat(timeline.getEventType(), equalTo(updatedTimeline.getEventType()));
            assertThat(updatedTimeline.isDeleted(), is(true));
        }
    }

    @Test
    public void whenCleanupTimelinesAndCacheFailedToUpdateThenTimelineStateIsReverted() {
        final Timeline t1 = createTimeline("et1", "topic1");

        final ImmutableList<Timeline> expiredTimelines = ImmutableList.of(t1);
        when(timelineDbRepository.getExpiredTimelines()).thenReturn(expiredTimelines);

        final TopicRepository topicRepository = mock(TopicRepository.class);
        when(timelineService.getTopicRepository(eq(t1))).thenReturn(topicRepository);

        doThrow(new RuntimeException()).when(eventTypeCache).invalidate(any());

        timelineCleanupJob.cleanupTimelines();

        verify(timelineDbRepository, times(2)).updateTimelime(any());
        assertThat(t1.isDeleted(), is(false));
    }

    private Timeline createTimeline(final String et, final String topic) {
        return new Timeline(et, 0, null, topic, null);
    }

}
