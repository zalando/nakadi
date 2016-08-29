package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.util.UUIDGenerator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.zalando.nakadi.utils.RandomSubscriptionBuilder.randomSubscription;
import static org.zalando.nakadi.utils.TestUtils.createRandomSubscriptions;

public class SubscriptionDbRepositoryTest extends AbstractDbRepositoryTest {

    private static final Comparator<Subscription> SUBSCRIPTION_CREATION_DATE_DESC_COMPARATOR =
            (sub1, sub2) -> sub2.getCreatedAt().compareTo(sub1.getCreatedAt());

    private SubscriptionDbRepository repository;

    public SubscriptionDbRepositoryTest() {
        super("zn_data.subscription");
    }

    @Before
    public void setUp() {
        super.setUp();
        repository = new SubscriptionDbRepository(template, mapper, new UUIDGenerator());
    }

    @Test
    public void whenCreateSubscriptionThenOk() throws Exception {

        final SubscriptionBase subscription = randomSubscription().build();
        final Subscription createdSubscription = repository.createSubscription(subscription);
        checkSubscriptionCreatedFromSubscriptionBase(createdSubscription, subscription);

        final int rows = template.queryForObject("SELECT count(*) FROM zn_data.subscription", Integer.class);
        assertThat("Number of rows should be 1", rows, equalTo(1));

        final Map<String, Object> result =
                template.queryForMap("SELECT s_id, s_subscription_object FROM zn_data.subscription");
        assertThat("Id is persisted", result.get("s_id"), equalTo(createdSubscription.getId()));

        final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();
        final Subscription saved = mapper.readValue(result.get("s_subscription_object").toString(), Subscription.class);
        assertThat("Saved subscription equal to original one", saved, equalTo(createdSubscription));
    }

    @Test(expected = DuplicatedSubscriptionException.class)
    public void whenCreateSubscriptionWithDuplicatedKeyParamsThenDuplicatedSubscriptionException() throws Exception {

        final SubscriptionBase subscription = randomSubscription().build();
        repository.createSubscription(subscription);

        // try to create subscription second time
        repository.createSubscription(subscription);
    }

    @Test
    public void whenGetSubscriptionByIdThenOk() throws Exception {

        // insert subscription into DB
        final Subscription subscription = randomSubscription().build();
        insertSubscriptionToDB(subscription);

        // get subscription by id and compare to original
        final Subscription gotSubscription = repository.getSubscription(subscription.getId());
        assertThat("We found the needed subscription", gotSubscription, equalTo(subscription));
    }

    @Test
    public void whenGetSubscriptionByKeyPropertiesThenOk() throws Exception {

        // insert subscription into DB
        final Subscription subscription = randomSubscription()
                .withOwningApplication("myapp")
                .withEventTypes(ImmutableSet.of("my-et", "second-et"))
                .withConsumerGroup("my-cg")
                .build();
        insertSubscriptionToDB(subscription);

        // get subscription by key properties and compare to original
        final Subscription gotSubscription = repository.getSubscription("myapp", ImmutableSet.of("second-et", "my-et"),
                "my-cg");
        assertThat("We found the needed subscription", gotSubscription, equalTo(subscription));
    }

    @Test
    public void whenListSubscriptionsThenOk() throws ServiceUnavailableException {

        final List<Subscription> testSubscriptions = Lists.newArrayList(
                randomSubscription().build(), randomSubscription().build());
        testSubscriptions.sort(SUBSCRIPTION_CREATION_DATE_DESC_COMPARATOR);
        testSubscriptions.forEach(this::insertSubscriptionToDB);

        final List<Subscription> subscriptions = repository.listSubscriptions(emptySet(), Optional.empty(), 0, 10);
        assertThat(subscriptions, equalTo(testSubscriptions));
    }

    @Test
    public void whenListSubscriptionsByOwningApplicationAndEventTypeThenOk() throws ServiceUnavailableException {

        final List<Subscription> testSubscriptions = ImmutableList.of(
                randomSubscription().withOwningApplication("app").withEventType("et1").build(),
                randomSubscription().withOwningApplication("app").withEventTypes(ImmutableSet.of("et2", "et1")).build(),
                randomSubscription().withOwningApplication("app").withEventType("et1").build(),
                randomSubscription().withOwningApplication("app").withEventType("et2").build(),
                randomSubscription().withOwningApplication("app").withEventTypes(ImmutableSet.of("et2", "et3")).build(),
                randomSubscription().withOwningApplication("app2").withEventType("et1").build(),
                randomSubscription().withOwningApplication("app2").withEventType("et2").build());
        testSubscriptions.forEach(this::insertSubscriptionToDB);

        final List<Subscription> expectedSubscriptions = testSubscriptions.stream()
                .filter(sub -> "app".equals(sub.getOwningApplication()) && sub.getEventTypes().contains("et1"))
                .sorted(SUBSCRIPTION_CREATION_DATE_DESC_COMPARATOR)
                .collect(toList());

        final List<Subscription> subscriptions = repository.listSubscriptions(ImmutableSet.of("et1"),
                Optional.of("app"), 0, 10);
        assertThat(subscriptions, equalTo(expectedSubscriptions));
    }

    @Test
    public void whenListSubscriptionsByMultipleEventTypesThenOk() throws ServiceUnavailableException {
        final List<Subscription> testSubscriptions = ImmutableList.of(
                randomSubscription().withEventTypes(ImmutableSet.of("et1", "et2")).build(),
                randomSubscription().withEventTypes(ImmutableSet.of("et1", "et2", "et3")).build(),
                randomSubscription().withEventTypes(ImmutableSet.of("et1")).build(),
                randomSubscription().withEventTypes(ImmutableSet.of("et2")).build(),
                randomSubscription().withEventTypes(ImmutableSet.of("et3", "et4", "et5")).build());
        testSubscriptions.forEach(this::insertSubscriptionToDB);

        final List<Subscription> expectedSubscriptions = testSubscriptions.stream()
                .filter(sub -> sub.getEventTypes().containsAll(ImmutableSet.of("et1", "et2")))
                .sorted(SUBSCRIPTION_CREATION_DATE_DESC_COMPARATOR)
                .collect(toList());

        final List<Subscription> subscriptions = repository.listSubscriptions(ImmutableSet.of("et1", "et2"),
                Optional.empty(), 0, 10);
        assertThat(subscriptions, equalTo(expectedSubscriptions));
    }

    @Test
    public void whenListSubscriptionsLimitAndOffsetAreRespected() throws ServiceUnavailableException {
        final List<Subscription> testSubscriptions = createRandomSubscriptions(10);
        testSubscriptions.forEach(this::insertSubscriptionToDB);

        final List<Subscription> expectedSubscriptions = testSubscriptions.stream()
                .sorted(SUBSCRIPTION_CREATION_DATE_DESC_COMPARATOR)
                .collect(toList());
        expectedSubscriptions.subList(0, 2).clear();
        expectedSubscriptions.subList(3, expectedSubscriptions.size()).clear();

        final List<Subscription> subscriptions = repository.listSubscriptions(emptySet(), Optional.empty(), 2, 3);
        assertThat(subscriptions, equalTo(expectedSubscriptions));
    }

    private void insertSubscriptionToDB(final Subscription subscription) {
        try {
            template.update("INSERT INTO zn_data.subscription (s_id, s_subscription_object) VALUES (?, ?::JSONB)",
                    subscription.getId(), mapper.writer().writeValueAsString(subscription));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void checkSubscriptionCreatedFromSubscriptionBase(final Subscription subscription,
                                                              final SubscriptionBase subscriptionBase) {
        assertThat(subscription.getId(), not(isEmptyOrNullString()));
        assertThat(subscription.getCreatedAt(), notNullValue());
        assertThat(subscription.getConsumerGroup(), equalTo(subscriptionBase.getConsumerGroup()));
        assertThat(subscription.getEventTypes(), equalTo(subscriptionBase.getEventTypes()));
        assertThat(subscription.getOwningApplication(), equalTo(subscriptionBase.getOwningApplication()));
        assertThat(subscription.getStartFrom(), equalTo(subscriptionBase.getStartFrom()));
    }

}
