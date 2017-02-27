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
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.NoEventTypeException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.exceptions.runtime.WrongInitialCursorsException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.view.Cursor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class SubscriptionValidationService {

    private final EventTypeRepository eventTypeRepository;
    private final TopicRepository topicRepository;
    private final int maxSubscriptionPartitions;

    @Autowired
    public SubscriptionValidationService(final TopicRepository topicRepository,
                                         final EventTypeRepository eventTypeRepository,
                                         final NakadiSettings nakadiSettings) {
        this.topicRepository = topicRepository;
        this.eventTypeRepository = eventTypeRepository;
        this.maxSubscriptionPartitions = nakadiSettings.getMaxSubscriptionPartitions();
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
            validateInitialCursors(subscription, allPartitions, eventTypes);
        }
    }

    private void validateInitialCursors(final SubscriptionBase subscription,
                                        final List<EventTypePartition> allPartitions,
                                        final List<EventType> eventTypes)
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
                    "there should be not more than 1 cursor for each partition in initial_cursors");
        }

        final Map<String, String> etTopics = eventTypes.stream()
                .collect(Collectors.toMap(
                        EventType::getName,
                        EventType::getTopic
                ));
        final List<NakadiCursor> cursorsWithoutBegin = subscription.getInitialCursors()
                .stream()
                .filter(c -> !Cursor.BEFORE_OLDEST_OFFSET.equals(c.getOffset()))
                .map(c -> new NakadiCursor(etTopics.get(c.getEventType()), c.getPartition(), c.getOffset()))
                .collect(Collectors.toList());

        try {
            topicRepository.validateCursors(cursorsWithoutBegin);
        } catch (InvalidCursorException e) {
            throw new WrongInitialCursorsException(e.getMessage(), e);
        } catch (ServiceUnavailableException e) {
            throw new RepositoryProblemException("Topic repository problem occurred when validating cursors", e);
        }
    }

    private List<EventTypePartition> getAllPartitions(final List<EventType> eventTypes) {
        return eventTypes.stream()
                .flatMap(et -> topicRepository.listPartitionNames(et.getTopic()).stream()
                        .map(p -> new EventTypePartition(et.getName(), p)))
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
