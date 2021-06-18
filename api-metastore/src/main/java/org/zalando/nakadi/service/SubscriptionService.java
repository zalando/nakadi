package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.opentracing.Span;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PaginationLinks;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.domain.PartitionBaseStatistics;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.exceptions.runtime.AuthorizationNotPresentException;
import org.zalando.nakadi.exceptions.runtime.DbWriteOperationsBlockedException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.SubscriptionUpdateConflictException;
import org.zalando.nakadi.exceptions.runtime.TooManyPartitionsException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.WrongInitialCursorsException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.db.SubscriptionTokenLister;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.service.subscription.LogPathBuilder;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionNode;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.zalando.nakadi.service.SubscriptionsUriHelper.createSubscriptionListLink;

@Component
public class SubscriptionService {

    private static final UriComponentsBuilder SUBSCRIPTION_PATH = UriComponentsBuilder.fromPath("/subscriptions/{id}");

    private final SubscriptionDbRepository subscriptionRepository;
    private final SubscriptionClientFactory subscriptionClientFactory;
    private final TimelineService timelineService;
    private final SubscriptionValidationService subscriptionValidationService;
    private final CursorConverter converter;
    private final CursorOperationsService cursorOperationsService;
    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final FeatureToggleService featureToggleService;
    private final String subLogEventType;
    private final SubscriptionTimeLagService subscriptionTimeLagService;
    private final AuthorizationValidator authorizationValidator;
    private final NakadiAuditLogPublisher nakadiAuditLogPublisher;
    private final EventTypeCache eventTypeCache;
    private final SubscriptionTokenLister subscriptionTokenLister;

    @Autowired
    public SubscriptionService(final SubscriptionDbRepository subscriptionRepository,
                               final SubscriptionClientFactory subscriptionClientFactory,
                               final TimelineService timelineService,
                               final SubscriptionValidationService subscriptionValidationService,
                               final CursorConverter converter,
                               final CursorOperationsService cursorOperationsService,
                               final NakadiKpiPublisher nakadiKpiPublisher,
                               final FeatureToggleService featureToggleService,
                               final SubscriptionTimeLagService subscriptionTimeLagService,
                               @Value("${nakadi.kpi.event-types.nakadiSubscriptionLog}") final String subLogEventType,
                               final NakadiAuditLogPublisher nakadiAuditLogPublisher,
                               final AuthorizationValidator authorizationValidator,
                               final EventTypeCache eventTypeCache,
                               final SubscriptionTokenLister subscriptionTokenLister) {
        this.subscriptionRepository = subscriptionRepository;
        this.subscriptionClientFactory = subscriptionClientFactory;
        this.timelineService = timelineService;
        this.subscriptionValidationService = subscriptionValidationService;
        this.converter = converter;
        this.cursorOperationsService = cursorOperationsService;
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.featureToggleService = featureToggleService;
        this.subscriptionTimeLagService = subscriptionTimeLagService;
        this.subLogEventType = subLogEventType;
        this.nakadiAuditLogPublisher = nakadiAuditLogPublisher;
        this.authorizationValidator = authorizationValidator;
        this.eventTypeCache = eventTypeCache;
        this.subscriptionTokenLister = subscriptionTokenLister;
    }

    public Subscription createSubscription(final SubscriptionBase subscriptionBase)
            throws TooManyPartitionsException, RepositoryProblemException, DuplicatedSubscriptionException,
            NoSuchEventTypeException, InconsistentStateException, WrongInitialCursorsException,
            DbWriteOperationsBlockedException, UnableProcessException,
            AuthorizationNotPresentException, ServiceTemporarilyUnavailableException {

        checkFeatureTogglesForCreationAndUpdate(subscriptionBase);

        subscriptionValidationService.validateSubscription(subscriptionBase);

        final Subscription subscription = subscriptionRepository.createSubscription(subscriptionBase);
        authorizationValidator.authorizeSubscriptionView(subscription);

        nakadiKpiPublisher.publish(subLogEventType, () -> new JSONObject()
                .put("subscription_id", subscription.getId())
                .put("status", "created"));

        nakadiAuditLogPublisher.publish(Optional.empty(), Optional.of(subscription),
                NakadiAuditLogPublisher.ResourceType.SUBSCRIPTION, NakadiAuditLogPublisher.ActionType.CREATED,
                subscription.getId());

        return subscription;
    }

    private void checkFeatureTogglesForCreationAndUpdate(final SubscriptionBase subscription) {
        if (featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot create subscription: write operations on DB " +
                    "are blocked by feature flag.");
        }
        if (featureToggleService.isFeatureEnabled(Feature.FORCE_SUBSCRIPTION_AUTHZ)
                && subscription.getAuthorization() == null) {
            throw new AuthorizationNotPresentException("Authorization section is mandatory");
        }
    }

    public Subscription updateSubscription(final String subscriptionId, final SubscriptionBase newValue)
            throws NoSuchSubscriptionException, SubscriptionUpdateConflictException {

        checkFeatureTogglesForCreationAndUpdate(newValue);

        final Subscription old = subscriptionRepository.getSubscription(subscriptionId);
        authorizationValidator.authorizeSubscriptionView(old);

        authorizationValidator.authorizeSubscriptionAdmin(old);

        subscriptionValidationService.validateSubscriptionChange(old, newValue);
        final Subscription updated = old.mergeFrom(newValue);
        subscriptionRepository.updateSubscription(updated);

        nakadiAuditLogPublisher.publish(Optional.of(old), Optional.of(updated),
                NakadiAuditLogPublisher.ResourceType.SUBSCRIPTION, NakadiAuditLogPublisher.ActionType.UPDATED,
                updated.getId());

        return updated;
    }

    public Subscription getExistingSubscription(final SubscriptionBase subscriptionBase)
            throws InconsistentStateException, NoSuchSubscriptionException, RepositoryProblemException {
        final Subscription subscription = subscriptionRepository.getSubscription(
                subscriptionBase.getOwningApplication(),
                subscriptionBase.getEventTypes(),
                subscriptionBase.getConsumerGroup());
        authorizationValidator.authorizeSubscriptionView(subscription);
        return subscription;
    }

    public UriComponents getSubscriptionUri(final Subscription subscription) {
        return SUBSCRIPTION_PATH.buildAndExpand(subscription.getId());
    }

    public PaginationWrapper<Subscription> listSubscriptions(@Nullable final String owningApplication,
                                                             @Nullable final Set<String> eventTypes,
                                                             final boolean showStatus,
                                                             final int limit,
                                                             final int offset,
                                                             final String token)
            throws InvalidLimitException, ServiceTemporarilyUnavailableException {
        if (limit < 1 || limit > 1000) {
            throw new InvalidLimitException("'limit' parameter should have value between 1 and 1000");
        }

        if (offset < 0) {
            throw new InvalidLimitException("'offset' parameter can't be lower than 0");
        }

        final Set<String> eventTypesFilter = eventTypes == null ? ImmutableSet.of() : eventTypes;
        final Optional<String> owningAppOption = Optional.ofNullable(owningApplication);
        SubscriptionTokenLister.Token tokenObj = null;
        // Here we are basically trying to support 3 situations
        // - In case if feature is not enabled, but token is provided - use token
        // - In case if feature is enabled but service is handcrafting offset - use offset instead of token
        //   This behavior is actually buggy, because first and other pages will use different sorting criteria.
        // - In case if feature is enabled and there is no handcraft - try to use token.
        if (!StringUtils.isEmpty(token) ||
                (0 == offset && featureToggleService.isFeatureEnabled(Feature.TOKEN_SUBSCRIPTIONS_ITERATION))) {
            if ("new".equalsIgnoreCase(token)) { // in order to test without feature toggle
                // TODO: remove handling of "new"
                tokenObj = SubscriptionTokenLister.Token.createEmpty();
            } else {
                tokenObj = SubscriptionTokenLister.Token.parse(token);
            }
        }
        final PaginationWrapper<Subscription> paginationWrapper;
        if (tokenObj == null) {
            final List<Subscription> subscriptions = subscriptionRepository.listSubscriptions(
                    eventTypesFilter,
                    owningAppOption,
                    offset,
                    limit);
            final Optional<PaginationLinks.Link> prev = Optional.of(offset).filter(v -> v > 0)
                    .map(o -> createSubscriptionListLink(
                            owningAppOption, eventTypesFilter, Math.max(0, o - limit), Optional.empty(), limit, 
                            showStatus));
            final Optional<PaginationLinks.Link> next = Optional.of(subscriptions.size()).filter(v -> v >= limit)
                    .map(size -> createSubscriptionListLink(
                            owningAppOption, eventTypesFilter, offset + size, Optional.empty(), limit, showStatus));

            paginationWrapper = new PaginationWrapper<>(subscriptions, new PaginationLinks(prev, next));
        } else {
            final SubscriptionTokenLister.ListResult listResult = subscriptionTokenLister.listSubscriptions(
                    eventTypesFilter, owningAppOption, tokenObj, limit);
            final Optional<PaginationLinks.Link> prev = Optional.ofNullable(listResult.getPrev())
                    .map(t -> createSubscriptionListLink(
                            owningAppOption, eventTypesFilter, 0, Optional.of(t), limit, showStatus));
            final Optional<PaginationLinks.Link> next = Optional.ofNullable(listResult.getNext())
                    .map(t -> createSubscriptionListLink(
                            owningAppOption, eventTypesFilter, 0, Optional.of(t), limit, showStatus));
            paginationWrapper = new PaginationWrapper<>(listResult.getItems(), new PaginationLinks(prev, next));
        }
        if (showStatus) {
            final List<Subscription> items = paginationWrapper.getItems();
            items.forEach(s -> s.setStatus(createSubscriptionStat(s, StatsMode.LIGHT)));
        }
        return paginationWrapper;
    }

    public Subscription getSubscription(final String subscriptionId)
            throws NoSuchSubscriptionException, ServiceTemporarilyUnavailableException {
        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
        authorizationValidator.authorizeSubscriptionView(subscription);
        return subscription;
    }

    public void deleteSubscription(final String subscriptionId)
            throws DbWriteOperationsBlockedException, NoSuchSubscriptionException, NoSuchEventTypeException,
            ServiceTemporarilyUnavailableException, InternalNakadiException {
        if (featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot delete subscription: write operations on DB " +
                    "are blocked by feature flag.");
        }
        final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
        authorizationValidator.authorizeSubscriptionView(subscription);

        authorizationValidator.authorizeSubscriptionAdmin(subscription);

        subscriptionRepository.deleteSubscription(subscriptionId);
        try (ZkSubscriptionClient zkSubscriptionClient = subscriptionClientFactory.createClient(
                subscription, LogPathBuilder.build(subscriptionId, "delete_subscription"))) {
            zkSubscriptionClient.deleteSubscription();
        } catch (IOException io) {
            throw new ServiceTemporarilyUnavailableException(io.getMessage(), io);
        }

        nakadiKpiPublisher.publish(subLogEventType, () -> new JSONObject()
                .put("subscription_id", subscriptionId)
                .put("status", "deleted"));

        nakadiAuditLogPublisher.publish(Optional.of(subscription), Optional.empty(),
                NakadiAuditLogPublisher.ResourceType.SUBSCRIPTION, NakadiAuditLogPublisher.ActionType.DELETED,
                subscription.getId());
    }

    public ItemsWrapper<SubscriptionEventTypeStats> getSubscriptionStat(final String subscriptionId,
                                                                        final StatsMode statsMode,
                                                                        final Span span)
            throws InconsistentStateException, NoSuchEventTypeException,
            NoSuchSubscriptionException, ServiceTemporarilyUnavailableException {
        final Subscription subscription;
        try {
            subscription = subscriptionRepository.getSubscription(subscriptionId);
            authorizationValidator.authorizeSubscriptionView(subscription);
        } catch (final ServiceTemporarilyUnavailableException ex) {
            TracingService.logErrorInSpan(span, ex);
            throw new InconsistentStateException(ex.getMessage());
        }
        final List<SubscriptionEventTypeStats> subscriptionStat = createSubscriptionStat(subscription, statsMode);
        return new ItemsWrapper<>(subscriptionStat);
    }

    private List<SubscriptionEventTypeStats> createSubscriptionStat(final Subscription subscription,
                                                                    final StatsMode statsMode)
            throws InconsistentStateException, NoSuchEventTypeException, ServiceTemporarilyUnavailableException {
        final List<EventType> eventTypes = getEventTypesForSubscription(subscription);
        subscriptionValidationService.verifyViewAccessOnEventTypes(eventTypes);
        try (ZkSubscriptionClient subscriptionClient = subscriptionClientFactory.createClient(subscription,
                LogPathBuilder.build(subscription.getId(), "stats"))) {
            final Optional<ZkSubscriptionNode> zkSubscriptionNode = subscriptionClient.getZkSubscriptionNode();

            if (statsMode == StatsMode.LIGHT) {
                return loadLightStats(eventTypes, zkSubscriptionNode);
            } else {
                return loadStats(eventTypes, zkSubscriptionNode, subscriptionClient, statsMode);
            }
        } catch (IOException io) {
            throw new ServiceTemporarilyUnavailableException(io.getMessage(), io);
        }
    }


    private List<EventType> getEventTypesForSubscription(final Subscription subscription)
            throws NoSuchEventTypeException {
        return subscription.getEventTypes().stream()
                .map(Try.wrap(eventTypeCache::getEventType))
                .map(Try::getOrThrow)
                .sorted(Comparator.comparing(EventType::getName))
                .collect(Collectors.toList());
    }

    private List<PartitionEndStatistics> loadPartitionEndStatistics(final Collection<EventType> eventTypes)
            throws ServiceTemporarilyUnavailableException {
        final List<PartitionEndStatistics> topicPartitions = new ArrayList<>();

        final Map<TopicRepository, List<Timeline>> timelinesByRepo = getTimelinesByRepository(eventTypes);

        for (final Map.Entry<TopicRepository, List<Timeline>> repoEntry : timelinesByRepo.entrySet()) {
            final TopicRepository topicRepository = repoEntry.getKey();
            final List<Timeline> timelinesForRepo = repoEntry.getValue();
            topicPartitions.addAll(topicRepository.loadTopicEndStatistics(timelinesForRepo));
        }
        return topicPartitions;
    }

    private Map<TopicRepository, List<Timeline>> getTimelinesByRepository(final Collection<EventType> eventTypes) {
        return eventTypes.stream()
                .map(timelineService::getActiveTimeline)
                .collect(Collectors.groupingBy(timelineService::getTopicRepository));
    }

    private List<String> getPartitionsList(final EventType eventType) {
        final Timeline activeTimeline = timelineService.getActiveTimeline(eventType);
        return timelineService.getTopicRepository(activeTimeline).listPartitionNames(activeTimeline.getTopic());
    }

    private List<SubscriptionEventTypeStats> loadStats(
            final Collection<EventType> eventTypes,
            final Optional<ZkSubscriptionNode> subscriptionNode,
            final ZkSubscriptionClient client, final StatsMode statsMode)
            throws ServiceTemporarilyUnavailableException, InconsistentStateException {
        final List<SubscriptionEventTypeStats> result = new ArrayList<>(eventTypes.size());
        final Collection<NakadiCursor> committedPositions = getCommittedPositions(subscriptionNode, client);
        final List<PartitionEndStatistics> stats = loadPartitionEndStatistics(eventTypes);

        final Map<EventTypePartition, Duration> timeLags = statsMode == StatsMode.TIMELAG ?
                subscriptionTimeLagService.getTimeLags(committedPositions, stats) :
                ImmutableMap.of();

        for (final EventType eventType : eventTypes) {
            final List<PartitionBaseStatistics> statsForEventType = stats.stream()
                    .filter(s -> s.getTimeline().getEventType().equals(eventType.getName()))
                    .collect(Collectors.toList());
            result.add(getEventTypeStats(subscriptionNode, eventType.getName(), statsForEventType,
                    committedPositions, timeLags));
        }
        return result;
    }

    private List<SubscriptionEventTypeStats> loadLightStats(final Collection<EventType> eventTypes,
                                                            final Optional<ZkSubscriptionNode> subscriptionNode)
            throws ServiceTemporarilyUnavailableException {
        final List<SubscriptionEventTypeStats> result = new ArrayList<>(eventTypes.size());
        for (final EventType eventType : eventTypes) {
            result.add(getEventTypeLightStats(subscriptionNode, eventType));
        }
        return result;
    }

    private SubscriptionEventTypeStats getEventTypeStats(final Optional<ZkSubscriptionNode> subscriptionNode,
                                                         final String eventTypeName,
                                                         final List<? extends PartitionBaseStatistics> stats,
                                                         final Collection<NakadiCursor> committedPositions,
                                                         final Map<EventTypePartition, Duration> timeLags) {
        final List<SubscriptionEventTypeStats.Partition> resultPartitions =
                new ArrayList<>(stats.size());
        for (final PartitionBaseStatistics stat : stats) {
            final String partition = stat.getPartition();
            final NakadiCursor lastPosition = ((PartitionEndStatistics) stat).getLast();
            final Long distance = computeDistance(committedPositions, lastPosition);
            final Long lagSeconds = Optional.ofNullable(timeLags.get(new EventTypePartition(eventTypeName, partition)))
                    .map(Duration::getSeconds)
                    .orElse(null);
            resultPartitions.add(getPartitionStats(subscriptionNode, eventTypeName, partition, distance, lagSeconds));
        }
        resultPartitions.sort(Comparator.comparing(SubscriptionEventTypeStats.Partition::getPartition));
        return new SubscriptionEventTypeStats(eventTypeName, resultPartitions);
    }

    private SubscriptionEventTypeStats getEventTypeLightStats(final Optional<ZkSubscriptionNode> subscriptionNode,
                                                              final EventType eventType) {
        final List<SubscriptionEventTypeStats.Partition> resultPartitions = new ArrayList<>();

        final List<String> partitionsList = subscriptionNode.map(
                node -> node.getPartitions().stream()
                        .map(Partition::getPartition)
                        .collect(Collectors.toList()))
                .orElseGet(() -> getPartitionsList(eventType));

        for (final String partition : partitionsList) {
            resultPartitions.add(getPartitionStats(subscriptionNode, eventType.getName(), partition, null, null));
        }
        resultPartitions.sort(Comparator.comparing(SubscriptionEventTypeStats.Partition::getPartition));
        return new SubscriptionEventTypeStats(eventType.getName(), resultPartitions);
    }

    private SubscriptionEventTypeStats.Partition getPartitionStats(final Optional<ZkSubscriptionNode> subscriptionNode,
                                                                   final String eventTypeName, final String partition,
                                                                   final Long distance, final Long lagSeconds) {
        final Partition.State state = getState(subscriptionNode, eventTypeName, partition);
        final String streamId = getStreamId(subscriptionNode, eventTypeName, partition);
        final SubscriptionEventTypeStats.Partition.AssignmentType assignmentType =
                getAssignmentType(subscriptionNode, eventTypeName, partition);
        return new SubscriptionEventTypeStats.Partition(partition, state.getDescription(),
                distance, lagSeconds, streamId, assignmentType);
    }

    private Collection<NakadiCursor> getCommittedPositions(final Optional<ZkSubscriptionNode> subscriptionNode,
                                                           final ZkSubscriptionClient client) {
        return subscriptionNode
                .map(node -> loadCommittedPositions(node.getPartitions(), client))
                .orElse(Collections.emptyList());
    }

    private Partition.State getState(final Optional<ZkSubscriptionNode> subscriptionNode, final String eventType,
                                     final String partition) {
        return subscriptionNode.map(node -> node.guessState(eventType, partition))
                .orElse(Partition.State.UNASSIGNED);
    }

    private SubscriptionEventTypeStats.Partition.AssignmentType getAssignmentType(
            final Optional<ZkSubscriptionNode> subscriptionNode, final String eventType, final String partition) {
        return subscriptionNode
                .map(node -> node.getPartitionAssignmentType(eventType, partition))
                .orElse(null);
    }

    private String getStreamId(final Optional<ZkSubscriptionNode> subscriptionNode,
                               final String eventType, final String partition) {
        return subscriptionNode
                .map(node -> node.guessStream(eventType, partition))
                .orElse("");
    }

    private Long computeDistance(final Collection<NakadiCursor> committedPositions, final NakadiCursor lastPosition) {
        return committedPositions.stream()
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
    }

    private Collection<NakadiCursor> loadCommittedPositions(
            final Collection<Partition> partitions, final ZkSubscriptionClient client)
            throws ServiceTemporarilyUnavailableException {
        try {

            final Map<EventTypePartition, SubscriptionCursorWithoutToken> committed = client.getOffsets(
                    partitions.stream().map(Partition::getKey).collect(Collectors.toList()));

            return converter.convert(committed.values());
        } catch (final InternalNakadiException | NoSuchEventTypeException | InvalidCursorException e) {
            throw new ServiceTemporarilyUnavailableException(e);
        }
    }

    public enum StatsMode {
        LIGHT,
        NORMAL,
        TIMELAG
    }

}
