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
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.NoEventTypeException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.exceptions.runtime.WrongInitialCursorsException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.subscription.SubscriptionValidationService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
    private Client client;

    @Before
    public void setUp() throws InternalNakadiException {
        final NakadiSettings nakadiSettings = mock(NakadiSettings.class);
        when(nakadiSettings.getMaxSubscriptionPartitions()).thenReturn(MAX_SUBSCRIPTION_PARTITIONS);

        topicRepository = mock(TopicRepository.class);
        when(topicRepository.listPartitionNames(argThat(isOneOf(topicForET(ET1), topicForET(ET2), topicForET(ET3)))))
                .thenReturn(ImmutableList.of(P0));

        etRepo = mock(EventTypeRepository.class);
        when(etRepo.findByNameO(any())).thenAnswer(invocation -> {
            final String etName = (String) invocation.getArguments()[0];
            final EventType eventType = new EventType();
            eventType.setName(etName);
            eventType.setTopic(topicForET(etName));
            eventType.setReadScopes(scopesForET(etName));
            return Optional.of(eventType);
        });

        subscriptionValidationService = new SubscriptionValidationService(topicRepository, etRepo, nakadiSettings);

        subscriptionBase = new SubscriptionBase();
        subscriptionBase.setEventTypes(ImmutableSet.of(ET1, ET2, ET3));
        subscriptionBase.setReadFrom(SubscriptionBase.InitialPosition.CURSORS);

        client = mock(Client.class);
    }

    @Test(expected = InconsistentStateException.class)
    public void whenFindEventTypeThrowsInternalExceptionThenIncosistentState() throws Exception {
        when(etRepo.findByNameO(argThat(isOneOf(ET1, ET2, ET3)))).thenThrow(new InternalNakadiException(""));
        subscriptionValidationService.validateSubscription(subscriptionBase, client);
    }

    @Test
    public void whenNoEventTypeThenException() throws Exception {
        when(etRepo.findByNameO(argThat(isOneOf(ET1, ET3)))).thenReturn(Optional.empty());
        when(etRepo.findByNameO(ET2)).thenReturn(Optional.of(new EventType()));

        try {
            subscriptionValidationService.validateSubscription(subscriptionBase, client);
            fail("NoEventTypeException expected");
        } catch (final NoEventTypeException e) {
            final String expectedMessage =
                    String.format("Failed to create subscription, event type(s) not found: '%s', '%s'", ET1, ET3);
            assertThat(e.getMessage(), equalTo(expectedMessage));
        }
    }

    @Test
    public void whenValidatingThenScopesAreChecked() throws Exception {
        subscriptionBase.setReadFrom(SubscriptionBase.InitialPosition.BEGIN);
        subscriptionValidationService.validateSubscription(subscriptionBase, client);
        verify(client, times(1)).checkScopes(scopesForET(ET1));
        verify(client, times(1)).checkScopes(scopesForET(ET2));
        verify(client, times(1)).checkScopes(scopesForET(ET3));
    }

    @Test(expected = TooManyPartitionsException.class)
    public void whenTooManyPartitionsThenException() throws Exception {
        when(topicRepository.listPartitionNames(argThat(isOneOf(
                topicForET(ET1), topicForET(ET2), topicForET(ET3)))))
                .thenReturn(Collections.nCopies(4, P0)); // 4 x 3 = 12 > 10
        subscriptionValidationService.validateSubscription(subscriptionBase, client);
    }

    @Test
    public void whenCursorForSomePartitionIsMissingThenException() throws Exception {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(P0, "", ET1),
                new SubscriptionCursorWithoutToken(P0, "", ET3)
        ));
        try {
            subscriptionValidationService.validateSubscription(subscriptionBase, client);
            fail("WrongInitialCursorsException expected");
        } catch (final WrongInitialCursorsException e) {
            assertThat(e.getMessage(),
                    equalTo("initial_cursors should contain cursors for all partitions of subscription"));
        }
    }

    @Test
    public void whenCursorForWrongPartitionThenException() throws Exception {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(P0, "", ET1),
                new SubscriptionCursorWithoutToken(P0, "", ET2),
                new SubscriptionCursorWithoutToken(P0, "", ET3),
                new SubscriptionCursorWithoutToken(P0, "", "wrongET")
        ));
        try {
            subscriptionValidationService.validateSubscription(subscriptionBase, client);
            fail("WrongInitialCursorsException expected");
        } catch (final WrongInitialCursorsException e) {
            assertThat(e.getMessage(),
                    equalTo("initial_cursors should contain cursors only for partitions of this subscription"));
        }
    }

    @Test
    public void whenMoreThanOneCursorPerPartitionThenException() throws Exception {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(P0, "", ET1),
                new SubscriptionCursorWithoutToken(P0, "", ET2),
                new SubscriptionCursorWithoutToken(P0, "a", ET3),
                new SubscriptionCursorWithoutToken(P0, "b", ET3)
        ));
        try {
            subscriptionValidationService.validateSubscription(subscriptionBase, client);
            fail("WrongInitialCursorsException expected");
        } catch (final WrongInitialCursorsException e) {
            assertThat(e.getMessage(),
                    equalTo("there should be not more than 1 cursor for each partition in initial_cursors"));
        }
    }

    @Test(expected = WrongInitialCursorsException.class)
    public void whenInvalidCursorThenException() throws Exception {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(P0, "", ET1),
                new SubscriptionCursorWithoutToken(P0, "", ET2),
                new SubscriptionCursorWithoutToken(P0, "", ET3)
        ));
        doThrow(new InvalidCursorException(CursorError.INVALID_FORMAT)).when(topicRepository).validateCursors(any());
        subscriptionValidationService.validateSubscription(subscriptionBase, client);
    }

    @Test(expected = RepositoryProblemException.class)
    public void whenServiceUnavailableThenException() throws Exception {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(P0, "", ET1),
                new SubscriptionCursorWithoutToken(P0, "", ET2),
                new SubscriptionCursorWithoutToken(P0, "", ET3)
        ));
        doThrow(new ServiceUnavailableException("")).when(topicRepository).validateCursors(any());
        subscriptionValidationService.validateSubscription(subscriptionBase, client);
    }

    @Test
    public void whenValidatingOnlyNoneBeginCursorsValidatedInRepository() throws Exception {
        subscriptionBase.setInitialCursors(ImmutableList.of(
                new SubscriptionCursorWithoutToken(P0, "o1", ET1),
                new SubscriptionCursorWithoutToken(P0, Cursor.BEFORE_OLDEST_OFFSET, ET2),
                new SubscriptionCursorWithoutToken(P0, "o3", ET3)
        ));
        subscriptionValidationService.validateSubscription(subscriptionBase, client);
        final ImmutableList<NakadiCursor> nakadiCursors = ImmutableList.of(
                new NakadiCursor(topicForET(ET1), P0, "o1"),
                new NakadiCursor(topicForET(ET3), P0, "o3")
        );
        verify(topicRepository, times(1)).validateCursors(nakadiCursors);
    }

    private static String topicForET(final String etName) {
        return "topic_" + etName;
    }

    private static Set<String> scopesForET(final String etName) {
        return ImmutableSet.of("read_scope_for_" + etName);
    }

}