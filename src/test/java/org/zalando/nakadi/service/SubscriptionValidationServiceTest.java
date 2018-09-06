package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.exceptions.runtime.WrongInitialCursorsException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.subscription.SubscriptionValidationService;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SubscriptionValidationServiceTest {

    public static final int MAX_SUBSCRIPTION_PARTITIONS = 10;
    public static final String ET1 = "et1";
    public static final String ET2 = "et2";
    public static final String ET3 = "et3";
    public static final String P0 = "p0";

    private TopicRepository topicRepository;
    private EventTypeRepository etRepo;
    private SubscriptionValidationService subscriptionValidationService;
    private SubscriptionBase subscriptionBase;
    private CursorConverter cursorConverter;

    @Before
    public void setUp() throws InternalNakadiException {
        final NakadiSettings nakadiSettings = mock(NakadiSettings.class);
        when(nakadiSettings.getMaxSubscriptionPartitions()).thenReturn(MAX_SUBSCRIPTION_PARTITIONS);

        topicRepository = mock(TopicRepository.class);
        when(topicRepository.listPartitionNames(argThat(isOneOf(topicForET(ET1), topicForET(ET2), topicForET(ET3)))))
                .thenReturn(ImmutableList.of(P0));

        etRepo = mock(EventTypeRepository.class);
        final Map<String, EventType> eventTypes = new HashMap<>();
        for (final String etName : new String[]{ET1, ET2, ET3}) {
            final EventType eventType = new EventType();
            eventType.setName(etName);
            eventTypes.put(etName, eventType);
        }
        when(etRepo.findByNameO(any()))
                .thenAnswer(invocation -> Optional.ofNullable(eventTypes.get(invocation.getArguments()[0])));

        final TimelineService timelineService = mock(TimelineService.class);
        for (final EventType et : eventTypes.values()) {
            final Timeline timeline = mock(Timeline.class);
            when(timeline.getTopic()).thenReturn(topicForET(et.getName()));
            when(timeline.getEventType()).thenReturn(et.getName());
            when(timelineService.getActiveTimeline(eq(et.getName()))).thenReturn(timeline);
        }
        when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepository);
        when(timelineService.getTopicRepository((EventType) any())).thenReturn(topicRepository);
        cursorConverter = mock(CursorConverter.class);
        subscriptionValidationService = new SubscriptionValidationService(timelineService, etRepo, nakadiSettings,
                cursorConverter, null);

        subscriptionBase = new SubscriptionBase();
        subscriptionBase.setEventTypes(ImmutableSet.of(ET1, ET2, ET3));
        subscriptionBase.setReadFrom(SubscriptionBase.InitialPosition.CURSORS);
    }

    @Test(expected = InconsistentStateException.class)
    public void whenFindEventTypeThrowsInternalExceptionThenIncosistentState() throws Exception {
        when(etRepo.findByNameO(argThat(isOneOf(ET1, ET2, ET3)))).thenThrow(new InternalNakadiException(""));
        subscriptionValidationService.validateSubscription(subscriptionBase);
    }

    @Test
    public void whenNoEventTypeThenException() throws Exception {
        when(etRepo.findByNameO(argThat(isOneOf(ET1, ET3)))).thenReturn(Optional.empty());
        when(etRepo.findByNameO(ET2)).thenReturn(Optional.of(new EventType()));

        try {
            subscriptionValidationService.validateSubscription(subscriptionBase);
            fail("NoSuchEventTypeException expected");
        } catch (final NoSuchEventTypeException e) {
            final String expectedMessage =
                    String.format("Failed to create subscription, event type(s) not found: '%s', '%s'", ET1, ET3);
            assertThat(e.getMessage(), equalTo(expectedMessage));
        }
    }

    @Test(expected = TooManyPartitionsException.class)
    public void whenTooManyPartitionsThenException() {
        when(topicRepository.listPartitionNames(argThat(isOneOf(
                topicForET(ET1), topicForET(ET2), topicForET(ET3)))))
                .thenReturn(Collections.nCopies(4, P0)); // 4 x 3 = 12 > 10
        subscriptionValidationService.validateSubscription(subscriptionBase);
    }

    @Test
    public void whenCursorForSomePartitionIsMissingThenException() {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(ET1, P0, ""),
                new SubscriptionCursorWithoutToken(ET3, P0, "")
        ));
        try {
            subscriptionValidationService.validateSubscription(subscriptionBase);
            fail("WrongInitialCursorsException expected");
        } catch (final WrongInitialCursorsException e) {
            assertThat(e.getMessage(),
                    equalTo("initial_cursors should contain cursors for all partitions of subscription"));
        }
    }

    @Test
    public void whenCursorForWrongPartitionThenException() {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(ET1, P0, ""),
                new SubscriptionCursorWithoutToken(ET2, P0, ""),
                new SubscriptionCursorWithoutToken(ET3, P0, ""),
                new SubscriptionCursorWithoutToken("wrongET", P0, "")
        ));
        try {
            subscriptionValidationService.validateSubscription(subscriptionBase);
            fail("WrongInitialCursorsException expected");
        } catch (final WrongInitialCursorsException e) {
            assertThat(e.getMessage(),
                    equalTo("initial_cursors should contain cursors only for partitions of this subscription"));
        }
    }

    @Test
    public void whenMoreThanOneCursorPerPartitionThenException() {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(ET1, P0, ""),
                new SubscriptionCursorWithoutToken(ET2, P0, ""),
                new SubscriptionCursorWithoutToken(ET3, P0, "a"),
                new SubscriptionCursorWithoutToken(ET3, P0, "b")
        ));
        try {
            subscriptionValidationService.validateSubscription(subscriptionBase);
            fail("WrongInitialCursorsException expected");
        } catch (final WrongInitialCursorsException e) {
            assertThat(e.getMessage(),
                    equalTo("there should be no more than 1 cursor for each partition in initial_cursors"));
        }
    }

    @Test(expected = WrongInitialCursorsException.class)
    public void whenInvalidCursorThenException() throws Exception {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(ET1, P0, ""),
                new SubscriptionCursorWithoutToken(ET2, P0, ""),
                new SubscriptionCursorWithoutToken(ET3, P0, "")
        ));
        final NakadiCursor cursor = mockCursorWithTimeline();
        when(cursorConverter.convert((SubscriptionCursorWithoutToken) any())).thenReturn(cursor);
        doThrow(new InvalidCursorException(CursorError.INVALID_FORMAT))
                .when(topicRepository).validateReadCursors(any());
        subscriptionValidationService.validateSubscription(subscriptionBase);
    }

    @Test(expected = RepositoryProblemException.class)
    public void whenServiceUnavailableThenException() throws Exception {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(ET1, P0, ""),
                new SubscriptionCursorWithoutToken(ET2, P0, ""),
                new SubscriptionCursorWithoutToken(ET3, P0, "")
        ));
        final NakadiCursor cursor = mockCursorWithTimeline();
        when(cursorConverter.convert((SubscriptionCursorWithoutToken) any())).thenReturn(cursor);
        doThrow(new ServiceTemporarilyUnavailableException("")).when(topicRepository).validateReadCursors(any());
        subscriptionValidationService.validateSubscription(subscriptionBase);
    }

    private static NakadiCursor mockCursorWithTimeline() {
        final Timeline timeline = mock(Timeline.class);
        when(timeline.isDeleted()).thenReturn(false);
        final NakadiCursor cursor = mock(NakadiCursor.class);
        when(cursor.getTimeline()).thenReturn(timeline);
        return cursor;
    }

    private static String topicForET(final String etName) {
        return "topic_" + etName;
    }

}
