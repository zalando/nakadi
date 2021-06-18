package org.zalando.nakadi.repository.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.util.HashGenerator;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;

public class SubscriptionTokenListerTest extends AbstractDbRepositoryTest {
    private SubscriptionTokenLister subscriptionTokenLister;
    private static final HashGenerator HASH_GENERATOR = new HashGenerator();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        subscriptionTokenLister = new SubscriptionTokenLister(template, TestUtils.OBJECT_MAPPER);
    }

    @Test
    public void testPagingFullPage() {
        final Optional<String> owiningApp = Optional.of(TestUtils.randomUUID());
        // There are 7 subscriptions
        final List<Subscription> testSubscriptions = createTestSubscriptions(owiningApp.get(), 2);
        // 1. Check that there are no paging links if requested more or equal to current count
        SubscriptionTokenLister.ListResult result;

        result = subscriptionTokenLister.listSubscriptions(emptySet(), owiningApp, null, null, 2);
        assertThat(result.getPrev(), nullValue());
        assertThat(result.getNext(), nullValue());
        assertThat(result.getItems(), equalTo(testSubscriptions));

        result = subscriptionTokenLister.listSubscriptions(emptySet(), owiningApp, null, null, 3);
        assertThat(result.getPrev(), nullValue());
        assertThat(result.getNext(), nullValue());
        assertThat(result.getItems(), equalTo(testSubscriptions));
    }

    @Test
    public void testPagingNoData() {
        final SubscriptionTokenLister.ListResult result =
                subscriptionTokenLister.listSubscriptions(
                        emptySet(),
                        Optional.of(TestUtils.randomUUID()),
                        null,
                        null,
                        2
                );
        assertThat(result.getPrev(), nullValue());
        assertThat(result.getNext(), nullValue());
        Assert.assertTrue(result.getItems().isEmpty());
    }

    @Test
    public void testPaging() {
        final Optional<String> owiningApp = Optional.of(TestUtils.randomUUID());
        // There are 7 subscriptions
        final List<Subscription> testSubscriptions = createTestSubscriptions(owiningApp.get(), 7);

        SubscriptionTokenLister.ListResult result;

        // 1. Check that there are no paging links if requested more or equal to current count
        result = subscriptionTokenLister.listSubscriptions(emptySet(), owiningApp, null, null, 7);
        assertThat(result.getPrev(), nullValue());
        assertThat(result.getNext(), nullValue());
        assertThat(result.getItems(), equalTo(testSubscriptions));

        result = subscriptionTokenLister.listSubscriptions(emptySet(), owiningApp, null, null, 8);
        assertThat(result.getPrev(), nullValue());
        assertThat(result.getNext(), nullValue());
        assertThat(result.getItems(), equalTo(testSubscriptions));

        // 2. Check actual iteration
        result = subscriptionTokenLister.listSubscriptions(emptySet(), owiningApp, null, null, 3);
        assertThat(result.getPrev(), nullValue());
        assertThat(result.getNext(), notNullValue());
        assertThat(result.getItems(), equalTo(testSubscriptions.subList(0, 3)));

        // 2.1 Second page
        final SubscriptionTokenLister.Token secondPage = result.getNext();
        result = subscriptionTokenLister.listSubscriptions(emptySet(), owiningApp, null, secondPage, 3);
        assertThat(result.getPrev(), notNullValue());
        assertThat(result.getNext(), notNullValue());
        assertThat(result.getItems(), equalTo(testSubscriptions.subList(3, 6)));

        // 2.2 Second page backwards
        final SubscriptionTokenLister.ListResult backResult =
                subscriptionTokenLister.listSubscriptions(emptySet(), owiningApp, null, result.getPrev(), 3);
        assertThat(backResult.getPrev(), nullValue());
        assertThat(backResult.getNext(), equalTo(secondPage));
        assertThat(backResult.getItems(), equalTo(testSubscriptions.subList(0, 3)));
        // 2.3
        final SubscriptionTokenLister.Token lastPageToken = result.getNext();
        final SubscriptionTokenLister.ListResult lastPage1 = subscriptionTokenLister.listSubscriptions(
                emptySet(), owiningApp, null, lastPageToken, 3);
        assertThat(lastPage1.getNext(), nullValue());
        assertThat(lastPage1.getPrev(), notNullValue());
        assertThat(lastPage1.getItems(), equalTo(testSubscriptions.subList(6, 7)));
        final SubscriptionTokenLister.ListResult lastPage2 = subscriptionTokenLister.listSubscriptions(
                emptySet(), owiningApp, null, lastPageToken, 1);
        assertThat(lastPage2, equalTo(lastPage1));
    }

    private List<Subscription> createTestSubscriptions(final String owiningApp, final int count) {
        final List<Subscription> testSubscriptions = IntStream.range(0, count)
                .mapToObj(i -> RandomSubscriptionBuilder.builder()
                        .withOwningApplication(owiningApp).withEventType("et1").build())
                .collect(toList());
        testSubscriptions.forEach(this::insertSubscriptionToDB);
        Collections.sort(testSubscriptions, Comparator.comparing(Subscription::getId));
        return testSubscriptions;
    }

    @Test
    public void testListing() {
        // The test is a copy of SubscriptionDbRepositoryTest, that is to be deleted when deprecated method removed
        final String owningApp = TestUtils.randomUUID();
        final String owningApp2 = TestUtils.randomUUID();

        final List<Subscription> testSubscriptions = ImmutableList.of(
                RandomSubscriptionBuilder.builder().withOwningApplication(owningApp).withEventType("et1").build(),
                RandomSubscriptionBuilder.builder().withOwningApplication(owningApp)
                        .withEventTypes(ImmutableSet.of("et2", "et1")).build(),
                RandomSubscriptionBuilder.builder().withOwningApplication(owningApp).withEventType("et1").build(),
                RandomSubscriptionBuilder.builder().withOwningApplication(owningApp).withEventType("et2").build(),
                RandomSubscriptionBuilder.builder().withOwningApplication(owningApp)
                        .withEventTypes(ImmutableSet.of("et2", "et3")).build(),
                RandomSubscriptionBuilder.builder().withOwningApplication(owningApp2).withEventType("et1").build(),
                RandomSubscriptionBuilder.builder().withOwningApplication(owningApp2).withEventType("et2").build());
        testSubscriptions.forEach(this::insertSubscriptionToDB);

        final List<Subscription> expectedSubscriptions = testSubscriptions.stream()
                .filter(sub -> owningApp.equals(sub.getOwningApplication()) && sub.getEventTypes().contains("et1"))
                .sorted(Comparator.comparing(Subscription::getId))
                .collect(toList());

        final List<Subscription> subscriptions = subscriptionTokenLister.listSubscriptions(
                ImmutableSet.of("et1"), Optional.of(owningApp), null, null, 10).getItems();
        assertThat(subscriptions, equalTo(expectedSubscriptions));

    }


    @Test
    public void whenListSubscriptionsByMultipleEventTypesThenOk() throws ServiceTemporarilyUnavailableException {
        // The test is a copy of SubscriptionDbRepositoryTest, that is to be deleted when deprecated method removed
        final String et1 = TestUtils.randomUUID();
        final String et2 = TestUtils.randomUUID();
        final String et3 = TestUtils.randomUUID();
        final String et4 = TestUtils.randomUUID();
        final String et5 = TestUtils.randomUUID();

        final List<Subscription> testSubscriptions = ImmutableList.of(
                RandomSubscriptionBuilder.builder().withEventTypes(ImmutableSet.of(et1, et2)).build(),
                RandomSubscriptionBuilder.builder().withEventTypes(ImmutableSet.of(et1, et2, et3)).build(),
                RandomSubscriptionBuilder.builder().withEventTypes(ImmutableSet.of(et1)).build(),
                RandomSubscriptionBuilder.builder().withEventTypes(ImmutableSet.of(et2)).build(),
                RandomSubscriptionBuilder.builder().withEventTypes(ImmutableSet.of(et3, et4, et5)).build());
        testSubscriptions.forEach(this::insertSubscriptionToDB);

        final List<Subscription> expectedSubscriptions = testSubscriptions.stream()
                .filter(sub -> sub.getEventTypes().containsAll(ImmutableSet.of(et1, et2)))
                .sorted(Comparator.comparing(Subscription::getId))
                .collect(toList());

        final List<Subscription> subscriptions = subscriptionTokenLister.listSubscriptions(ImmutableSet.of(et1, et2),
                Optional.empty(), null, null, 10).getItems();
        assertThat(subscriptions, equalTo(expectedSubscriptions));
    }

    private void insertSubscriptionToDB(final Subscription subscription) {
        try {
            template.update("INSERT INTO zn_data.subscription (s_id, s_subscription_object, s_key_fields_hash) " +
                            "VALUES (?, ?::JSONB, ?)",
                    subscription.getId(),
                    TestUtils.OBJECT_MAPPER.writer().writeValueAsString(subscription),
                    HASH_GENERATOR.generateSubscriptionKeyFieldsHash(subscription));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
