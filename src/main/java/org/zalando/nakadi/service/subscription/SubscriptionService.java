package org.zalando.nakadi.service.subscription;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PaginationLinks;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.exceptions.runtime.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.exceptions.runtime.NoEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.exceptions.runtime.WrongInitialCursorsException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionNode;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.SubscriptionsUriHelper;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;
import org.zalando.problem.Problem;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class SubscriptionService {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionService.class);
    private static final UriComponentsBuilder SUBSCRIPTION_PATH = UriComponentsBuilder.fromPath("/subscriptions/{id}");

    private final SubscriptionDbRepository subscriptionRepository;
    private final EventTypeRepository eventTypeRepository;
    private final SubscriptionClientFactory subscriptionClientFactory;
    private final TimelineService timelineService;
    private final SubscriptionValidationService subscriptionValidationService;
    private final CursorConverter converter;
    private final CursorOperationsService cursorOperationsService;

    @Autowired
    public SubscriptionService(final SubscriptionDbRepository subscriptionRepository,
                               final SubscriptionClientFactory subscriptionClientFactory,
                               final TimelineService timelineService,
                               final EventTypeRepository eventTypeRepository,
                               final SubscriptionValidationService subscriptionValidationService,
                               final CursorConverter converter,
                               final CursorOperationsService cursorOperationsService) {
        this.subscriptionRepository = subscriptionRepository;
        this.subscriptionClientFactory = subscriptionClientFactory;
        this.timelineService = timelineService;
        this.eventTypeRepository = eventTypeRepository;
        this.subscriptionValidationService = subscriptionValidationService;
        this.converter = converter;
        this.cursorOperationsService = cursorOperationsService;
    }

    public Subscription createSubscription(final SubscriptionBase subscriptionBase)
            throws TooManyPartitionsException, RepositoryProblemException, DuplicatedSubscriptionException,
            NoEventTypeException, InconsistentStateException, WrongInitialCursorsException {

        subscriptionValidationService.validateSubscription(subscriptionBase);
        return subscriptionRepository.createSubscription(subscriptionBase);
    }

    public Subscription getExistingSubscription(final SubscriptionBase subscriptionBase)
            throws InconsistentStateException, NoSubscriptionException, RepositoryProblemException {
        return subscriptionRepository.getSubscription(
                subscriptionBase.getOwningApplication(),
                subscriptionBase.getEventTypes(),
                subscriptionBase.getConsumerGroup());
    }

    public UriComponents getSubscriptionUri(final Subscription subscription) {
        return SUBSCRIPTION_PATH.buildAndExpand(subscription.getId());
    }

    public Result listSubscriptions(@Nullable final String owningApplication, @Nullable final Set<String> eventTypes,
                                    final int limit, final int offset) {
        if (limit < 1 || limit > 1000) {
            final Problem problem = Problem.valueOf(Response.Status.BAD_REQUEST,
                    "'limit' parameter should have value from 1 to 1000");
            return Result.problem(problem);
        }

        if (offset < 0) {
            final Problem problem = Problem.valueOf(Response.Status.BAD_REQUEST,
                    "'offset' parameter can't be lower than 0");
            return Result.problem(problem);
        }

        try {
            final Set<String> eventTypesFilter = eventTypes == null ? ImmutableSet.of() : eventTypes;
            final Optional<String> owningAppOption = Optional.ofNullable(owningApplication);
            final List<Subscription> subscriptions =
                    subscriptionRepository.listSubscriptions(eventTypesFilter, owningAppOption, offset, limit);
            final PaginationLinks paginationLinks = SubscriptionsUriHelper.createSubscriptionPaginationLinks(
                    owningAppOption, eventTypesFilter, offset, limit, subscriptions.size());
            return Result.ok(new PaginationWrapper<>(subscriptions, paginationLinks));
        } catch (final ServiceUnavailableException e) {
            LOG.error("Error occurred during listing of subscriptions: {}", e.getProblemMessage());
            return Result.problem(e.asProblem());
        }
    }

    public Result<Subscription> getSubscription(final String subscriptionId) {
        try {
            final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
            return Result.ok(subscription);
        } catch (final NoSuchSubscriptionException e) {
            LOG.debug("Failed to find subscription {}: {}", subscriptionId, e.getProblemMessage());
            return Result.problem(e.asProblem());
        } catch (final ServiceUnavailableException e) {
            LOG.error("Error occurred when trying to get subscription {}: {}", subscriptionId, e.getProblemMessage());
            return Result.problem(e.asProblem());
        }
    }

    public Result<Void> deleteSubscription(final String subscriptionId) {
        try {
            final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);

            subscriptionRepository.deleteSubscription(subscriptionId);
            final ZkSubscriptionClient zkSubscriptionClient = subscriptionClientFactory.createClient(
                    subscription, "subscription." + subscriptionId + ".delete_subscription");
            zkSubscriptionClient.deleteSubscription();

            return Result.ok();
        } catch (final NoSuchSubscriptionException e) {
            LOG.debug("Failed to find subscription {}: {}", subscriptionId, e.getProblemMessage());
            return Result.problem(e.asProblem());
        } catch (final ServiceUnavailableException e) {
            LOG.error("Error occurred when trying to delete subscription {}: {}",
                    subscriptionId, e.getProblemMessage());
            return Result.problem(e.asProblem());
        } catch (final NoSuchEventTypeException | InternalNakadiException e) {
            LOG.error("Exception cannot occur: {}", e.getProblemMessage());
            return Result.problem(e.asProblem());
        }
    }

    public ItemsWrapper<SubscriptionEventTypeStats> getSubscriptionStat(final String subscriptionId)
            throws InconsistentStateException, NoSuchSubscriptionException, ServiceTemporarilyUnavailableException {
        final Subscription subscription;
        try {
            subscription = subscriptionRepository.getSubscription(subscriptionId);
        } catch (final ServiceUnavailableException ex) {
            throw new InconsistentStateException(ex.getMessage());
        }
        final List<SubscriptionEventTypeStats> subscriptionStat = createSubscriptionStat(subscription);
        return new ItemsWrapper<>(subscriptionStat);
    }


    private List<SubscriptionEventTypeStats> createSubscriptionStat(final Subscription subscription)
            throws InconsistentStateException, ServiceTemporarilyUnavailableException {

        final List<EventType> eventTypes = subscription.getEventTypes().stream()
                .map(Try.wrap(eventTypeRepository::findByName))
                .map(Try::getOrThrow)
                .sorted(Comparator.comparing(EventType::getName))
                .collect(Collectors.toList());

        final List<PartitionEndStatistics> topicPartitions;
        try {
            topicPartitions = loadPartitionEndStatistics(eventTypes);
        } catch (final ServiceUnavailableException ex) {
            throw new ServiceTemporarilyUnavailableException(ex);
        }

        final ZkSubscriptionClient subscriptionClient;
        try {
            subscriptionClient = subscriptionClientFactory.createClient(
                    subscription, "subscription." + subscription.getId() + ".stats");
        } catch (final InternalNakadiException | NoSuchEventTypeException e) {
            throw new ServiceTemporarilyUnavailableException(e);
        }

        final ZkSubscriptionNode zkSubscriptionNode = subscriptionClient.getZkSubscriptionNodeLocked();

        return loadStats(eventTypes, zkSubscriptionNode, subscriptionClient, topicPartitions);
    }

    private List<PartitionEndStatistics> loadPartitionEndStatistics(final Collection<EventType> eventTypes)
            throws ServiceUnavailableException {
        final List<PartitionEndStatistics> topicPartitions = new ArrayList<>();

        final Map<TopicRepository, List<Timeline>> timelinesByRepo = eventTypes.stream()
                .map(timelineService::getActiveTimeline)
                .collect(Collectors.groupingBy(timelineService::getTopicRepository));

        for (final Map.Entry<TopicRepository, List<Timeline>> repoEntry : timelinesByRepo.entrySet()) {
            final TopicRepository topicRepository = repoEntry.getKey();
            final List<Timeline> timelinesForRepo = repoEntry.getValue();
            topicPartitions.addAll(topicRepository.loadTopicEndStatistics(timelinesForRepo));
        }
        return topicPartitions;
    }

    private List<SubscriptionEventTypeStats> loadStats(
            final Collection<EventType> eventTypes,
            final ZkSubscriptionNode subscriptionNode,
            final ZkSubscriptionClient client,
            final List<PartitionEndStatistics> stats)
            throws ServiceTemporarilyUnavailableException, InconsistentStateException {
        final List<SubscriptionEventTypeStats> result = new ArrayList<>(eventTypes.size());
        final List<NakadiCursor> committedPositions;
        try {
            committedPositions = loadCommittedPositions(subscriptionNode, client);
        } catch (final InternalNakadiException | NoSuchEventTypeException | InvalidCursorException |
                ServiceUnavailableException e) {
            throw new ServiceTemporarilyUnavailableException(e);
        }

        for (final EventType eventType : eventTypes) {
            final List<SubscriptionEventTypeStats.Partition> resultPartitions = new ArrayList<>(stats.size());
            for (final PartitionEndStatistics stat : stats) {
                final NakadiCursor lastPosition = stat.getLast();
                if (!lastPosition.getEventType().equals(eventType.getName())) {
                    continue;
                }
                final Long distance = committedPositions.stream()
                        .filter(pos -> pos.getEventTypePartition().equals(lastPosition.getEventTypePartition()))
                        .findAny()
                        .map(committed -> {
                            try {
                                return cursorOperationsService.calculateDistance(committed, lastPosition);
                            } catch (final InvalidCursorOperation ex) {
                                throw new InconsistentStateException(
                                        "Unexpected exception while calculating distance", ex);
                            }
                        })
                        .orElse(null);

                final String state = subscriptionNode.guessState(stat.getTimeline().getEventType(), stat.getPartition())
                        .getDescription();
                final String streamId = Optional.ofNullable(subscriptionNode.guessStream(
                        stat.getTimeline().getEventType(), stat.getPartition())).orElse("");

                resultPartitions.add(new SubscriptionEventTypeStats.Partition(
                        lastPosition.getPartition(), state, distance, streamId));
            }
            resultPartitions.sort(Comparator.comparing(SubscriptionEventTypeStats.Partition::getPartition));
            result.add(new SubscriptionEventTypeStats(eventType.getName(), resultPartitions));
        }
        return result;
    }

    private List<NakadiCursor> loadCommittedPositions(
            final ZkSubscriptionNode subscriptionNode,
            final ZkSubscriptionClient client) throws InternalNakadiException, InvalidCursorException,
            NoSuchEventTypeException, ServiceUnavailableException {
        final List<SubscriptionCursorWithoutToken> views = Stream.of(subscriptionNode.getPartitions()).map(
                partition -> client.getOffset(partition.getKey()))
                .collect(Collectors.toList());
        return converter.convert(views);
    }

}
