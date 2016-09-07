package org.zalando.nakadi.service.subscription;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.PaginationLinks;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.domain.SubscriptionListWrapper;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClientFactory;
import org.zalando.nakadi.util.SubscriptionsUriHelper;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class SubscriptionService {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionService.class);
    private static final UriComponentsBuilder SUBSCRIPTION_PATH = UriComponentsBuilder.fromPath("/subscriptions/{id}");

    private final SubscriptionDbRepository subscriptionRepository;
    private final EventTypeRepository eventTypeRepository;
    private final ZkSubscriptionClientFactory zkSubscriptionClientFactory;
    private final TopicRepository topicRepository;

    @Autowired
    public SubscriptionService(final SubscriptionDbRepository subscriptionRepository,
                               final ZkSubscriptionClientFactory zkSubscriptionClientFactory,
                               final TopicRepository topicRepository,
                               final EventTypeRepository eventTypeRepository) {
        this.subscriptionRepository = subscriptionRepository;
        this.zkSubscriptionClientFactory = zkSubscriptionClientFactory;
        this.topicRepository = topicRepository;
        this.eventTypeRepository = eventTypeRepository;
    }

    public Result<Subscription> createSubscription(final SubscriptionBase subscriptionBase, final Client client)
            throws DuplicatedSubscriptionException {
        try {
            return createSubscriptionInternal(subscriptionBase, client);
        } catch (final ServiceUnavailableException e) {
            LOG.error("Error occurred during subscription creation", e);
            return Result.problem(e.asProblem());
        } catch (final InternalNakadiException e) {
            LOG.error("Error occurred during subscription creation", e);
            return Result.problem(e.asProblem());
        }
    }

    private Result<Subscription> createSubscriptionInternal(final SubscriptionBase subscriptionBase, final Client
            client) throws InternalNakadiException, DuplicatedSubscriptionException, ServiceUnavailableException {
        final Map<String, Optional<EventType>> eventTypeMapping =
                subscriptionBase.getEventTypes().stream()
                        .collect(Collectors.toMap(Function.identity(),
                                Try.wrap(eventTypeRepository::findByNameO).andThen(Try::getOrThrow)));
        final List<String> missingEventTypes = eventTypeMapping.entrySet().stream()
                .filter(entry -> !entry.getValue().isPresent())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (!missingEventTypes.isEmpty()) {
            final Problem problem = Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY,
                    createMissingEventsErrorMessage(missingEventTypes));
            return Result.problem(problem);
        }

        eventTypeMapping.values().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(eventType -> client.checkScopes(eventType.getReadScopes()));

        // generate subscription id and try to create subscription in DB
        final Subscription subscription = subscriptionRepository.createSubscription(subscriptionBase);
        return Result.ok(subscription);
    }

    public Result<Subscription> processDuplicatedSubscription(final SubscriptionBase subscriptionBase) {
        try {
            final Subscription existingSubscription = getExistingSubscription(subscriptionBase);
            return Result.ok(existingSubscription);
        } catch (final ServiceUnavailableException ex) {
            LOG.error("Error occurred during fetching existing subscription", ex);
            return Result.problem(ex.asProblem());
        } catch (final NoSuchSubscriptionException | InternalNakadiException ex) {
            LOG.error("Error occurred during fetching existing subscription", ex);
            final Problem problem = Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR, ex.getProblemMessage());
            return Result.problem(problem);
        }
    }

    private Subscription getExistingSubscription(final SubscriptionBase subscriptionBase)
            throws NoSuchSubscriptionException, InternalNakadiException, ServiceUnavailableException {
        return subscriptionRepository.getSubscription(
                subscriptionBase.getOwningApplication(),
                subscriptionBase.getEventTypes(),
                subscriptionBase.getConsumerGroup());
    }

    private String createMissingEventsErrorMessage(final List<String> missingEventTypes) {
        return new StringBuilder()
                .append("Failed to create subscription, event type(s) not found: '")
                .append(StringUtils.join(missingEventTypes, "','"))
                .append("'").toString();
    }

    public UriComponents getSubscriptionUri(final Subscription subscription) {
        final UriComponents path = SUBSCRIPTION_PATH.buildAndExpand(subscription.getId());
        return path;
    }

    public Result listSubscriptions(final String owningApplication, final Set<String> eventTypes, final int limit,
                                    final int offset) {
        if(limit< 1 || limit >1000) {
            final Problem problem = Problem.valueOf(Response.Status.BAD_REQUEST,
                    "'limit' parameter should have value from 1 to 1000");
            return Result.problem(problem);
        }

        if(offset < 0) {
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
            return Result.ok(new SubscriptionListWrapper(subscriptions, paginationLinks));
        } catch (ServiceUnavailableException e) {
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
        final Partition[] partitions = zkSubscriptionClient.listPartitions();

        final List<EventType> eventTypes = subscription.getEventTypes().stream()
                .map(Try.wrap(eventTypeRepository::findByName))
                .map(Try::getOrThrow)
                .collect(Collectors.toList());

        final Set<String> topics = eventTypes.stream()
                .map(EventType::getTopic)
                .collect(Collectors.toSet());

        final List<TopicPartition> topicPartitions = topicRepository.listPartitions(topics);

        return eventTypes.stream()
                .map(eventType -> {
                    final Set<SubscriptionEventTypeStats.Partition> statPartitions = Arrays.stream(partitions)
                            .filter(partition -> eventType.getTopic().equals(partition.getKey().getTopic()))
                            .flatMap(partition ->
                                    filterTopicPartitions(zkSubscriptionClient, topicPartitions, partition))
                            .collect(Collectors.toSet());
                    return new SubscriptionEventTypeStats(eventType.getName(), statPartitions);
                })
                .collect(Collectors.toList());
    }

    private Stream<SubscriptionEventTypeStats.Partition> filterTopicPartitions(
            final ZkSubscriptionClient zkSubscriptionClient,
            final List<TopicPartition> topicPartitions,
            final Partition partition) {
        return topicPartitions.stream()
                .filter(topicPartition ->
                        partition.getKey().getPartition().equals(topicPartition.getPartitionId()))
                .map(Try.wrap(topicPartition ->
                        createPartition(zkSubscriptionClient, partition, topicPartition)))
                .map(Try::getOrThrow);
    }

    private SubscriptionEventTypeStats.Partition createPartition(final ZkSubscriptionClient zkSubscriptionClient,
                                                                 final Partition partition,
                                                                 final TopicPartition topicPartition)
            throws NakadiException {
        final String partitionName = partition.getKey().getPartition();
        final String partitionState = partition.getState().getDescription();
        final String partitionSession = partition.getSession();
        final long clientOffset = zkSubscriptionClient.getOffset(partition.getKey());
        final long total = Long.valueOf(topicPartition.getNewestAvailableOffset());
        final long unconsumedEvents = total - clientOffset;
        return new SubscriptionEventTypeStats.Partition(
                partitionName, partitionState, unconsumedEvents, partitionSession);
    }
}
