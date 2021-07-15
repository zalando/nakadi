package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Feature;
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
import org.zalando.nakadi.exceptions.runtime.InvalidInitialCursorsException;
import org.zalando.nakadi.exceptions.runtime.InvalidOwningApplicationException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.validation.ResourceValidationHelperService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

@RunWith(MockitoJUnitRunner.class)
public class SubscriptionValidationServiceTest {

    public static final int MAX_SUBSCRIPTION_PARTITIONS = 10;
    public static final String ET1 = "et1";
    public static final String ET2 = "et2";
    public static final String ET3 = "et3";
    public static final String P0 = "p0";

    private static final String OWNING_APP_WRONG = "wrong_owning_app";
    private static final String OWNING_APP_CORRECT = "correct_owning_app";

    @Mock
    private TopicRepository topicRepository;
    @Mock
    private EventTypeCache etCache;
    private SubscriptionValidationService subscriptionValidationService;
    private SubscriptionBase subscriptionBase;
    @Mock
    private CursorConverter cursorConverter;
    @Mock
    private TimelineService timelineService;
    @Mock
    private NakadiSettings nakadiSettings;
    @Mock
    private AuthorizationValidator authorizationValidator;
    @Mock
    private FeatureToggleService featureToggleService;
    @Mock
    private ApplicationService applicationService;

    @Before
    public void setUp() throws InternalNakadiException {
        when(nakadiSettings.getMaxSubscriptionPartitions()).thenReturn(MAX_SUBSCRIPTION_PARTITIONS);
        when(topicRepository.listPartitionNames(argThat(isOneOf(topicForET(ET1), topicForET(ET2), topicForET(ET3)))))
                .thenReturn(ImmutableList.of(P0));

        final Map<String, EventType> eventTypes = new HashMap<>();
        for (final String etName : new String[]{ET1, ET2, ET3}) {
            final EventType eventType = new EventType();
            eventType.setName(etName);
            eventTypes.put(etName, eventType);
        }
        when(etCache.getEventTypeIfExists(any()))
                .thenAnswer(invocation -> Optional.ofNullable(eventTypes.get(invocation.getArguments()[0])));

        for (final EventType et : eventTypes.values()) {
            final Timeline timeline = mock(Timeline.class);
            when(timeline.getTopic()).thenReturn(topicForET(et.getName()));
            when(timeline.getEventType()).thenReturn(et.getName());
            when(timelineService.getActiveTimeline(eq(et.getName()))).thenReturn(timeline);
        }
        when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepository);

        when(applicationService.exists(eq(OWNING_APP_WRONG))).thenReturn(false);
        when(applicationService.exists(eq(OWNING_APP_CORRECT))).thenReturn(true);

        cursorConverter = mock(CursorConverter.class);
        final ResourceValidationHelperService validationHelperService = new ResourceValidationHelperService();
        subscriptionValidationService = new SubscriptionValidationService(timelineService, nakadiSettings,
                cursorConverter, authorizationValidator, etCache, featureToggleService, applicationService,
                validationHelperService);

        subscriptionBase = new SubscriptionBase();
        subscriptionBase.setEventTypes(ImmutableSet.of(ET1, ET2, ET3));
        subscriptionBase.setReadFrom(SubscriptionBase.InitialPosition.CURSORS);
        subscriptionBase.setOwningApplication(OWNING_APP_CORRECT);
    }

    @Test(expected = InconsistentStateException.class)
    public void whenFindEventTypeThrowsInternalExceptionThenIncosistentState() {
        when(etCache.getEventTypeIfExists(argThat(isOneOf(ET1, ET2, ET3)))).thenThrow(new InternalNakadiException(""));
        subscriptionValidationService.validateSubscriptionOnCreate(subscriptionBase);
    }

    @Test
    public void whenNoEventTypeThenException() {
        when(etCache.getEventTypeIfExists(argThat(isOneOf(ET1, ET3)))).thenReturn(Optional.empty());
        when(etCache.getEventTypeIfExists(ET2)).thenReturn(Optional.of(new EventType()));

        try {
            subscriptionValidationService.validateSubscriptionOnCreate(subscriptionBase);
            fail("NoSuchEventTypeException expected");
        } catch (final NoSuchEventTypeException e) {
            final String expectedMessage =
                    String.format("Failed to create subscription, event type(s) not found: '%s', '%s'", ET1, ET3);
            assertThat(e.getMessage(), equalTo(expectedMessage));
        }
    }

    @Test(expected = InvalidOwningApplicationException.class)
    public void whenWrongOwningApplicationWithFTThenFail() {
        subscriptionBase.setReadFrom(SubscriptionBase.InitialPosition.END);
        subscriptionBase.setOwningApplication(OWNING_APP_WRONG);
        when(featureToggleService.isFeatureEnabled(eq(Feature.VALIDATE_SUBSCRIPTION_OWNING_APPLICATION)))
                .thenReturn(true);
        subscriptionValidationService.validateSubscriptionOnCreate(subscriptionBase);
    }

    @Test
    public void whenWrongOwningApplicationWithoutFTThenSuccess() {
        subscriptionBase.setReadFrom(SubscriptionBase.InitialPosition.END);
        subscriptionBase.setOwningApplication(OWNING_APP_WRONG);
        when(featureToggleService.isFeatureEnabled(eq(Feature.VALIDATE_SUBSCRIPTION_OWNING_APPLICATION)))
                .thenReturn(false);
        subscriptionValidationService.validateSubscriptionOnCreate(subscriptionBase);
    }

    @Test
    public void whenCorrectOwningApplicationWithFTThenSuccess() {
        subscriptionBase.setReadFrom(SubscriptionBase.InitialPosition.END);
        subscriptionBase.setOwningApplication(OWNING_APP_CORRECT);
        when(featureToggleService.isFeatureEnabled(eq(Feature.VALIDATE_SUBSCRIPTION_OWNING_APPLICATION)))
                .thenReturn(true);
        subscriptionValidationService.validateSubscriptionOnCreate(subscriptionBase);
    }

    @Test(expected = TooManyPartitionsException.class)
    public void whenTooManyPartitionsThenException() {
        when(topicRepository.listPartitionNames(argThat(isOneOf(
                topicForET(ET1), topicForET(ET2), topicForET(ET3)))))
                .thenReturn(Collections.nCopies(4, P0)); // 4 x 3 = 12 > 10
        subscriptionValidationService.validateSubscriptionOnCreate(subscriptionBase);
    }

    @Test
    public void whenCursorForSomePartitionIsMissingThenException() {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(ET1, P0, ""),
                new SubscriptionCursorWithoutToken(ET3, P0, "")
        ));
        try {
            subscriptionValidationService.validateSubscriptionOnCreate(subscriptionBase);
            fail("WrongInitialCursorsException expected");
        } catch (final InvalidInitialCursorsException e) {
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
            subscriptionValidationService.validateSubscriptionOnCreate(subscriptionBase);
            fail("WrongInitialCursorsException expected");
        } catch (final InvalidInitialCursorsException e) {
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
            subscriptionValidationService.validateSubscriptionOnCreate(subscriptionBase);
            fail("WrongInitialCursorsException expected");
        } catch (final InvalidInitialCursorsException e) {
            assertThat(e.getMessage(),
                    equalTo("there should be no more than 1 cursor for each partition in initial_cursors"));
        }
    }

    @Test(expected = InvalidInitialCursorsException.class)
    public void whenInvalidCursorThenException() {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(ET1, P0, ""),
                new SubscriptionCursorWithoutToken(ET2, P0, ""),
                new SubscriptionCursorWithoutToken(ET3, P0, "")
        ));
        final NakadiCursor cursor = mockCursorWithTimeline();
        when(cursorConverter.convert((SubscriptionCursorWithoutToken) any())).thenReturn(cursor);
        doThrow(new InvalidCursorException(CursorError.INVALID_FORMAT, ET1))
                .when(topicRepository).validateReadCursors(any());
        subscriptionValidationService.validateSubscriptionOnCreate(subscriptionBase);
    }

    @Test(expected = RepositoryProblemException.class)
    public void whenServiceUnavailableThenException() {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(ET1, P0, ""),
                new SubscriptionCursorWithoutToken(ET2, P0, ""),
                new SubscriptionCursorWithoutToken(ET3, P0, "")
        ));
        final NakadiCursor cursor = mockCursorWithTimeline();
        when(cursorConverter.convert((SubscriptionCursorWithoutToken) any())).thenReturn(cursor);
        doThrow(new ServiceTemporarilyUnavailableException("")).when(topicRepository).validateReadCursors(any());
        subscriptionValidationService.validateSubscriptionOnCreate(subscriptionBase);
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
