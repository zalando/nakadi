package org.zalando.nakadi.webservice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionCursor;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidStreamIdException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.CursorsService;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Charsets.UTF_8;
import static java.text.MessageFormat.format;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.TestUtils.randomUUID;
import static org.zalando.nakadi.utils.TestUtils.randomValidEventTypeName;

public class CursorsServiceAT extends BaseAT {

    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);
    private static final String SUBSCRIPTIONS_PATH = "/nakadi/subscriptions";

    private static final String NEW_OFFSET = "newOffset";
    private static final String OLD_OFFSET = "oldOffset";
    private static final String OLDEST_OFFSET = "oldestOffset";
    private static final String P1 = "p1";
    private static final String P2 = "p2";

    private CursorsService cursorsService;
    private EventTypeRepository eventTypeRepository;

    private String etName;
    private String topic;
    private String sid;
    private String streamId;
    private String token;
    private List<SubscriptionCursor> testCursors;

    @Before
    public void before() throws Exception {
        sid = randomUUID();
        streamId = randomUUID();
        etName = randomValidEventTypeName();
        topic = randomUUID();
        token = randomUUID();
        testCursors = ImmutableList.of(new SubscriptionCursor(P1, NEW_OFFSET, etName, token));

        final EventType eventType = mock(EventType.class);
        when(eventType.getTopic()).thenReturn(topic);

        eventTypeRepository = mock(EventTypeRepository.class);
        when(eventTypeRepository.findByName(etName)).thenReturn(eventType);

        final CursorTokenService tokenService = mock(CursorTokenService.class);
        when(tokenService.generateToken()).thenReturn(token);

        final ZooKeeperHolder zkHolder = mock(ZooKeeperHolder.class);
        when(zkHolder.get()).thenReturn(CURATOR);

        final TopicRepository topicRepository = mock(TopicRepository.class);
        when(topicRepository.compareOffsets(NEW_OFFSET, OLD_OFFSET)).thenReturn(1);
        when(topicRepository.compareOffsets(OLDEST_OFFSET, OLD_OFFSET)).thenReturn(-1);

        final Subscription subscription = mock(Subscription.class);
        when(subscription.getEventTypes()).thenReturn(ImmutableSet.of(etName));
        final SubscriptionDbRepository subscriptionRepo = mock(SubscriptionDbRepository.class);
        when(subscriptionRepo.getSubscription(sid)).thenReturn(subscription);

        cursorsService = new CursorsService(zkHolder, topicRepository, subscriptionRepo, eventTypeRepository,
                tokenService);

        // bootstrap data in ZK
        CURATOR.create().creatingParentsIfNeeded().forPath(offsetPath(P1), OLD_OFFSET.getBytes(UTF_8));
        CURATOR.setData().forPath(partitionPath(P1), (streamId + ": :ASSIGNED").getBytes(UTF_8));
        CURATOR.create().creatingParentsIfNeeded().forPath(offsetPath(P2), OLD_OFFSET.getBytes(UTF_8));
        CURATOR.setData().forPath(partitionPath(P2), (streamId + ": :ASSIGNED").getBytes(UTF_8));
        CURATOR.create().creatingParentsIfNeeded().forPath(sessionPath(streamId));
    }

    @After
    public void after() throws Exception {
        CURATOR.delete().deletingChildrenIfNeeded().forPath(subscriptionPath());
    }

    @Test
    public void whenCommitCursorsThenTrue() throws Exception {
        final Map<SubscriptionCursor, Boolean> commitResult = cursorsService.commitCursors(streamId, sid, testCursors);
        assertThat(commitResult, equalTo(ImmutableMap.of(testCursors.get(0), true)));
        checkCurrentOffsetInZk(P1, NEW_OFFSET);
    }

    @Test
    public void whenStreamIdInvalidThenException() throws Exception {
        try {
            cursorsService.commitCursors("wrong-stream-id", sid, testCursors);
            fail("Expected InvalidStreamIdException to be thrown");
        } catch (final InvalidStreamIdException ignore) {
        }
        checkCurrentOffsetInZk(P1, OLD_OFFSET);
    }

    @Test
    public void whenPartitionIsStreamedToDifferentClientThenFalse() throws Exception {
        CURATOR.setData().forPath(partitionPath(P1), ("wrong-stream-id" + ": :ASSIGNED").getBytes(UTF_8));
        try {
            cursorsService.commitCursors(streamId, sid, testCursors);
            fail("Expected InvalidStreamIdException to be thrown");
        } catch (final InvalidStreamIdException ignore) {
        }
        checkCurrentOffsetInZk(P1, OLD_OFFSET);
    }

    @Test
    public void whenCommitOldCursorsThenFalse() throws Exception {
        testCursors = ImmutableList.of(new SubscriptionCursor(P1, OLDEST_OFFSET, etName, token));
        final Map<SubscriptionCursor, Boolean> commitResult = cursorsService.commitCursors(streamId, sid, testCursors);
        assertThat(commitResult, equalTo(ImmutableMap.of(testCursors.get(0), false)));
        checkCurrentOffsetInZk(P1, OLD_OFFSET);
    }

    @Test
    public void whenETRepoExceptionThenException() throws Exception {
        when(eventTypeRepository.findByName(any())).thenThrow(new InternalNakadiException(""));
        try {
            cursorsService.commitCursors(streamId, sid, testCursors);
            fail("Expected InvalidStreamIdException to be thrown");
        } catch (final NakadiException ignore) {
        }
        checkCurrentOffsetInZk(P1, OLD_OFFSET);
    }

    @Test
    public void whenFirstCursorIsNotCommittedThenNextCursorsAreNotSkipped() throws Exception {
        final SubscriptionCursor c1 = new SubscriptionCursor(P1, OLDEST_OFFSET, etName, token);
        final SubscriptionCursor c2 = new SubscriptionCursor(P2, NEW_OFFSET, etName, token);
        testCursors = ImmutableList.of(c1, c2);

        final Map<SubscriptionCursor, Boolean> result = cursorsService.commitCursors(streamId, sid, testCursors);

        assertThat(result.get(c1), is(false));
        assertThat(result.get(c2), is(true));

        checkCurrentOffsetInZk(P1, OLD_OFFSET);
        checkCurrentOffsetInZk(P2, NEW_OFFSET);
    }

    @Test
    public void whenGetSubscriptionCursorsThenOk() throws Exception {
        final List<SubscriptionCursor> cursors = cursorsService.getSubscriptionCursors(sid);
        assertThat(ImmutableSet.copyOf(cursors),
                equalTo(ImmutableSet.of(
                        new SubscriptionCursor(P1, OLD_OFFSET, etName, token),
                        new SubscriptionCursor(P2, OLD_OFFSET, etName, token)
                )));
    }

    private void checkCurrentOffsetInZk(final String partition, final String offset) throws Exception {
        final String committedOffset = new String(CURATOR.getData().forPath(offsetPath(partition)), UTF_8);
        assertThat(committedOffset, equalTo(offset));
    }

    private String offsetPath(final String partition) {
        return format("{0}/{1}/topics/{2}/{3}/offset", SUBSCRIPTIONS_PATH, sid, topic, partition);
    }

    private String partitionPath(final String partition) {
        return format("{0}/{1}/topics/{2}/{3}", SUBSCRIPTIONS_PATH, sid, topic, partition);
    }

    private String sessionPath(final String sessionId) {
        return format("{0}/{1}/sessions/{2}", SUBSCRIPTIONS_PATH, sid, sessionId);
    }

    private String subscriptionPath() {
        return format("{0}/{1}", SUBSCRIPTIONS_PATH, sid);
    }

}
