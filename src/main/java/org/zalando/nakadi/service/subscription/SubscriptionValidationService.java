package org.zalando.nakadi.service.subscription;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.IllegalScopeException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.NoEventTypeException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.exceptions.runtime.WrongInitialCursorsException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class SubscriptionValidationService {

    private final EventTypeRepository eventTypeRepository;
    private final TimelineService timelineService;
    private final int maxSubscriptionPartitions;
    private final CursorConverter cursorConverter;

    @Autowired
    public SubscriptionValidationService(final TimelineService timelineService,
                                         final EventTypeRepository eventTypeRepository,
                                         final NakadiSettings nakadiSettings,
                                         final CursorConverter cursorConverter) {
        this.timelineService = timelineService;
        this.eventTypeRepository = eventTypeRepository;
        this.maxSubscriptionPartitions = nakadiSettings.getMaxSubscriptionPartitions();
        this.cursorConverter = cursorConverter;
    }

    public void validateSubscription(final SubscriptionBase subscription, final Client client)
            throws TooManyPartitionsException, RepositoryProblemException, NoEventTypeException,
            InconsistentStateException, WrongInitialCursorsException, IllegalScopeException {

        // check that all event-types exist
        final Map<String, Optional<EventType>> eventTypesOrNone = getSubscriptionEventTypesOrNone(subscription);
        checkEventTypesExist(eventTypesOrNone);

        final List<EventType> eventTypes = eventTypesOrNone.values().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        // check if application is allowed to read from the event-types
        eventTypes.forEach(eventType -> client.checkScopes(eventType.getReadScopes()));

        // check that maximum number of partitions is not exceeded
        final List<EventTypePartition> allPartitions = getAllPartitions(eventTypes);
        if (allPartitions.size() > maxSubscriptionPartitions) {
            final String message = String.format(
                    "total partition count for subscription is %d, but the maximum partition count is %d",
                    allPartitions.size(), maxSubscriptionPartitions);
            throw new TooManyPartitionsException(message);
        }

        // validate initial cursors if needed
        if (subscription.getReadFrom() == SubscriptionBase.InitialPosition.CURSORS) {
            validateInitialCursors(subscription, allPartitions);
        }
    }

    private void validateInitialCursors(final SubscriptionBase subscription,
                                        final List<EventTypePartition> allPartitions)
            throws WrongInitialCursorsException, RepositoryProblemException {

        final boolean cursorsMissing = allPartitions.stream()
                .anyMatch(p -> !subscription.getInitialCursors().stream().anyMatch(p::ownsCursor));
        if (cursorsMissing) {
            throw new WrongInitialCursorsException(
                    "initial_cursors should contain cursors for all partitions of subscription");
        }

        final boolean hasCursorForWrongPartition = subscription.getInitialCursors().stream()
                .anyMatch(c -> !allPartitions.contains(new EventTypePartition(c.getEventType(), c.getPartition())));
        if (hasCursorForWrongPartition) {
            throw new WrongInitialCursorsException(
                    "initial_cursors should contain cursors only for partitions of this subscription");
        }

        if (subscription.getInitialCursors().size() > allPartitions.size()) {
            throw new WrongInitialCursorsException(
                    "there should be no more than 1 cursor for each partition in initial_cursors");
        }

        try {
            for (final SubscriptionCursorWithoutToken cursor : subscription.getInitialCursors()) {
                final NakadiCursor nakadiCursor = cursorConverter.convert(cursor);
                timelineService.getTopicRepository(nakadiCursor.getTimeline()).validateReadCursors(
                        Collections.singletonList(nakadiCursor));
            }
        } catch (final InvalidCursorException ex) {
            throw new WrongInitialCursorsException(ex.getMessage(), ex);
        } catch (final NakadiException ex) {
            throw new RepositoryProblemException("Topic repository problem occurred when validating cursors", ex);
        }
    }

    private List<EventTypePartition> getAllPartitions(final List<EventType> eventTypes) {
        return eventTypes.stream()
                .map(timelineService::getTimeline)
                .flatMap(timeline -> timelineService.getTopicRepository(timeline)
                        .listPartitionNames(timeline.getTopic())
                        .stream()
                        .map(p -> new EventTypePartition(timeline.getEventType(), p)))
                .collect(Collectors.toList());
    }

    private Map<String, Optional<EventType>> getSubscriptionEventTypesOrNone(final SubscriptionBase subscriptionBase)
            throws InconsistentStateException {
        return subscriptionBase.getEventTypes().stream()
                .collect(Collectors.toMap(Function.identity(),
                        et -> {
                            try {
                                return eventTypeRepository.findByNameO(et);
                            } catch (InternalNakadiException e) {
                                throw new InconsistentStateException("Unexpected error when getting event type", e);
                            }
                        }));
    }

    private void checkEventTypesExist(final Map<String, Optional<EventType>> eventTypesOrNone)
            throws NoEventTypeException {
        final List<String> missingEventTypes = eventTypesOrNone.entrySet().stream()
                .filter(entry -> !entry.getValue().isPresent())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        if (!missingEventTypes.isEmpty()) {
            throw new NoEventTypeException(String.format("Failed to create subscription, event type(s) not found: '%s'",
                    StringUtils.join(missingEventTypes, "', '")));
        }
    }

}
