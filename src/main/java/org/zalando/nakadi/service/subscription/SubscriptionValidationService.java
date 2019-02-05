package org.zalando.nakadi.service.subscription;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.SubscriptionUpdateConflictException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.exceptions.runtime.WrongInitialCursorsException;
import org.zalando.nakadi.exceptions.runtime.WrongStreamParametersException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.zalando.nakadi.domain.CursorError.UNAVAILABLE;

@Service
public class SubscriptionValidationService {

    private final EventTypeRepository eventTypeRepository;
    private final TimelineService timelineService;
    private final int maxSubscriptionPartitions;
    private final CursorConverter cursorConverter;
    private final AuthorizationValidator authorizationValidator;

    @Autowired
    public SubscriptionValidationService(final TimelineService timelineService,
                                         final EventTypeRepository eventTypeRepository,
                                         final NakadiSettings nakadiSettings,
                                         final CursorConverter cursorConverter,
                                         final AuthorizationValidator authorizationValidator) {
        this.timelineService = timelineService;
        this.eventTypeRepository = eventTypeRepository;
        this.maxSubscriptionPartitions = nakadiSettings.getMaxSubscriptionPartitions();
        this.cursorConverter = cursorConverter;
        this.authorizationValidator = authorizationValidator;
    }

    public void validateSubscription(final SubscriptionBase subscription)
            throws TooManyPartitionsException, RepositoryProblemException, NoSuchEventTypeException,
            InconsistentStateException, WrongInitialCursorsException {

        // check that all event-types exist
        final Map<String, Optional<EventType>> eventTypesOrNone = getSubscriptionEventTypesOrNone(subscription);
        checkEventTypesExist(eventTypesOrNone);

        // check that maximum number of partitions is not exceeded
        final List<EventTypePartition> allPartitions = getAllPartitions(subscription.getEventTypes());
        if (allPartitions.size() > maxSubscriptionPartitions) {
            final String message = String.format(
                    "total partition count for subscription is %d, but the maximum partition count is %d",
                    allPartitions.size(), maxSubscriptionPartitions);
            throw new TooManyPartitionsException(message);
        }

        // checkStorageAvailability initial cursors if needed
        if (subscription.getReadFrom() == SubscriptionBase.InitialPosition.CURSORS) {
            validateInitialCursors(subscription, allPartitions);
        }
        // Verify that subscription authorization object is valid
        authorizationValidator.validateAuthorization(subscription.asBaseResource("new-subscription"));
    }

    public void validateSubscriptionChange(final Subscription old, final SubscriptionBase newValue)
            throws SubscriptionUpdateConflictException {
        if (!Objects.equals(newValue.getConsumerGroup(), old.getConsumerGroup())) {
            throw new SubscriptionUpdateConflictException("Not allowed to change subscription consumer group");
        }
        if (!Objects.equals(newValue.getEventTypes(), old.getEventTypes())) {
            throw new SubscriptionUpdateConflictException("Not allowed to change subscription event types");
        }
        if (!Objects.equals(newValue.getOwningApplication(), old.getOwningApplication())) {
            throw new SubscriptionUpdateConflictException("Not allowed to change owning application");
        }
        if (!Objects.equals(newValue.getReadFrom(), old.getReadFrom())) {
            throw new SubscriptionUpdateConflictException("Not allowed to change read from");
        }
        if (!Objects.equals(newValue.getInitialCursors(), old.getInitialCursors())) {
            throw new SubscriptionUpdateConflictException("Not allowed to change initial cursors");
        }
        authorizationValidator.validateAuthorization(old.asResource(), newValue.asBaseResource(old.getId()));
    }

    public void validatePartitionsToStream(final Subscription subscription, final List<EventTypePartition> partitions) {
        // check for duplicated partitions
        final long uniquePartitions = partitions.stream().distinct().count();
        if (uniquePartitions < partitions.size()) {
            throw new WrongStreamParametersException("Duplicated partition specified");
        }
        // check that partitions belong to subscription
        final List<EventTypePartition> allPartitions = getAllPartitions(subscription.getEventTypes());
        final List<EventTypePartition> wrongPartitions = partitions.stream()
                .filter(p -> !allPartitions.contains(p))
                .collect(Collectors.toList());
        if (!wrongPartitions.isEmpty()) {
            final String wrongPartitionsDesc = wrongPartitions.stream()
                    .map(EventTypePartition::toString)
                    .collect(Collectors.joining(", "));
            throw new WrongStreamParametersException("Wrong partitions specified - some partitions don't belong to " +
                    "subscription: " + wrongPartitionsDesc);
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
                if (nakadiCursor.getTimeline().isDeleted()) {
                    throw new InvalidCursorException(UNAVAILABLE, nakadiCursor);
                }
                timelineService.getTopicRepository(nakadiCursor.getTimeline()).validateReadCursors(
                        Collections.singletonList(nakadiCursor));
            }
        } catch (final InvalidCursorException ex) {
            throw new WrongInitialCursorsException(ex.getMessage(), ex);
        } catch (final InternalNakadiException | ServiceTemporarilyUnavailableException ex) {
            throw new RepositoryProblemException("Topic repository problem occurred when validating cursors", ex);
        }
    }

    private List<EventTypePartition> getAllPartitions(final Collection<String> eventTypes) {
        return eventTypes.stream()
                .map(timelineService::getActiveTimeline)
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
            throws NoSuchEventTypeException {
        final List<String> missingEventTypes = eventTypesOrNone.entrySet().stream()
                .filter(entry -> !entry.getValue().isPresent())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        if (!missingEventTypes.isEmpty()) {
            throw new NoSuchEventTypeException(String.format("Failed to create subscription, event type(s) not " +
                    "found: '%s'", StringUtils.join(missingEventTypes, "', '")));
        }
    }
}
