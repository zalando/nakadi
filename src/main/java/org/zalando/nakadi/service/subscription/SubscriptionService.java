package org.zalando.nakadi.service.subscription;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
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
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.IllegalScopeException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.exceptions.runtime.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.NoEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.exceptions.runtime.WrongInitialCursorsException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionNode;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.SubscriptionsUriHelper;
import org.zalando.nakadi.view.Cursor;
import org.zalando.problem.Problem;

@Component
public class SubscriptionService {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionService.class);
    private static final UriComponentsBuilder SUBSCRIPTION_PATH = UriComponentsBuilder.fromPath("/subscriptions/{id}");

    private final SubscriptionDbRepository subscriptionRepository;
    private final EventTypeRepository eventTypeRepository;
    private final ZkSubscriptionClientFactory zkSubscriptionClientFactory;
    private final TimelineService timelineService;
    private final SubscriptionValidationService subscriptionValidationService;

    @Autowired
    public SubscriptionService(final SubscriptionDbRepository subscriptionRepository,
                               final ZkSubscriptionClientFactory zkSubscriptionClientFactory,
                               final TimelineService timelineService,
                               final EventTypeRepository eventTypeRepository,
                               final SubscriptionValidationService subscriptionValidationService) {
        this.subscriptionRepository = subscriptionRepository;
        this.zkSubscriptionClientFactory = zkSubscriptionClientFactory;
        this.timelineService = timelineService;
        this.eventTypeRepository = eventTypeRepository;
        this.subscriptionValidationService = subscriptionValidationService;
    }

    public Subscription createSubscription(final SubscriptionBase subscriptionBase, final Client client)
            throws TooManyPartitionsException, RepositoryProblemException, DuplicatedSubscriptionException,
            NoEventTypeException, InconsistentStateException, WrongInitialCursorsException, IllegalScopeException {

        subscriptionValidationService.validateSubscription(subscriptionBase, client);
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
            return Result.ok(new PaginationWrapper(subscriptions, paginationLinks));
        } catch (final ServiceUnavailableException e) {
            LOG.error("Error occurred during listing of subscriptions", e);
            return Result.problem(e.asProblem());
        }
    }

    public Result<Subscription> getSubscription(final String subscriptionId) {
        try {
            final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
            return Result.ok(subscription);
        } catch (final NoSuchSubscriptionException e) {
            LOG.debug("Failed to find subscription: {}", subscriptionId, e);
            return Result.problem(e.asProblem());
        } catch (final ServiceUnavailableException e) {
            LOG.error("Error occurred when trying to get subscription: {}", subscriptionId, e);
            return Result.problem(e.asProblem());
        }
    }

    public Result<Void> deleteSubscription(final String subscriptionId, final Client client) {
        try {
            final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
            if (!client.idMatches(subscription.getOwningApplication())) {
                return Result.forbidden("You don't have access to this subscription");
            }

            subscriptionRepository.deleteSubscription(subscriptionId);

            final ZkSubscriptionClient zkSubscriptionClient =
                    zkSubscriptionClientFactory.createZkSubscriptionClient(subscriptionId);
            zkSubscriptionClient.deleteSubscription();

            return Result.ok();
        } catch (final NoSuchSubscriptionException e) {
            LOG.debug("Failed to find subscription: {}", subscriptionId, e);
            return Result.problem(e.asProblem());
        } catch (final ServiceUnavailableException e) {
            LOG.error("Error occurred when trying to delete subscription: {}", subscriptionId, e);
            return Result.problem(e.asProblem());
        }
    }

    public Result<ItemsWrapper<SubscriptionEventTypeStats>> getSubscriptionStat(final String subscriptionId) {
        try {
            final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
            final List<SubscriptionEventTypeStats> subscriptionStat = createSubscriptionStat(subscription);
            return Result.ok(new ItemsWrapper<>(subscriptionStat));
        } catch (final NoSuchSubscriptionException e) {
            LOG.debug("Failed to find subscription: {}", subscriptionId, e);
            return Result.problem(e.asProblem());
        } catch (final ServiceUnavailableException e) {
            LOG.error("Error occurred when trying to get subscription stat: {}" + subscriptionId, e);
            return Result.problem(e.asProblem());
        }
    }

    private List<SubscriptionEventTypeStats> createSubscriptionStat(final Subscription subscription)
            throws ServiceUnavailableException {
        final ZkSubscriptionClient zkSubscriptionClient =
                zkSubscriptionClientFactory.createZkSubscriptionClient(subscription.getId());
        final ZkSubscriptionNode zkSubscriptionNode = zkSubscriptionClient.getZkSubscriptionNodeLocked();


        final List<EventType> eventTypes = subscription.getEventTypes().stream()
                .map(Try.wrap(eventTypeRepository::findByName))
                .map(Try::getOrThrow)
                .collect(Collectors.toList());

        final List<PartitionStatistics> topicPartitions = new ArrayList<>();
        for (final EventType eventType : eventTypes) {
            final Timeline timeline = timelineService.getTimeline(eventType);
            topicPartitions.addAll(
                    timelineService.getTopicRepository(timeline)
                            .loadTopicStatistics(Collections.singletonList(timeline)));
        }

        return eventTypes.stream()
                .map(eventType -> {
                    final Set<SubscriptionEventTypeStats.Partition> statPartitions =
                            topicPartitions.stream()
                                    .filter(partition -> eventType.getTopic().equals(partition.getTopic()))
                                    .map(Try.wrap(partition ->
                                            mergePartitions(zkSubscriptionClient, zkSubscriptionNode, partition)))
                                    .map(Try::getOrThrow)
                                    .collect(Collectors.toCollection(() -> new TreeSet<>(
                                            Comparator.comparingInt(p -> Integer.valueOf(p.getPartition()))))
                                    );
                    return new SubscriptionEventTypeStats(eventType.getName(), statPartitions);
                })
                .collect(Collectors.toList());
    }

    private SubscriptionEventTypeStats.Partition mergePartitions(
            final ZkSubscriptionClient zkSubscriptionClient,
            final ZkSubscriptionNode zkSubscriptionNode,
            final PartitionStatistics topicPartition) throws NakadiException {
        final boolean hasSessions = zkSubscriptionNode.getSessions().length > 0;

        final Partition partition = Arrays.stream(zkSubscriptionNode.getPartitions())
                .filter(p -> p.getKey().getPartition().equals(topicPartition.getPartition())
                        && p.getKey().getTopic().equals(topicPartition.getTopic()))
                .findFirst()
                .orElse(null);

        return createPartition(zkSubscriptionClient, partition, topicPartition.getLast(), hasSessions);
    }

    private SubscriptionEventTypeStats.Partition createPartition(final ZkSubscriptionClient zkSubscriptionClient,
                                                                 @Nullable final Partition partition,
                                                                 final NakadiCursor topicPartition,
                                                                 final boolean hasSessions) throws NakadiException {
        final String partitionId = topicPartition.getPartition();
        String partitionState = Partition.State.UNASSIGNED.getDescription();
        String partitionSession = "";
        Long unconsumedEvents = null;
        if (partition != null) {
            if (hasSessions) {
                partitionState = partition.getState().getDescription();
                partitionSession = partition.getSession();
            }
            final String total = topicPartition.getOffset();
            if (!Cursor.BEFORE_OLDEST_OFFSET.equals(total)) {
                final String clientOffset = zkSubscriptionClient.getOffset(partition.getKey());
                // TODO! Use different service for this stuff
                unconsumedEvents = Long.valueOf(total) - Long.valueOf(clientOffset);
            }
        }
        return new SubscriptionEventTypeStats.Partition(
                partitionId, partitionState, unconsumedEvents, partitionSession);
    }
}
