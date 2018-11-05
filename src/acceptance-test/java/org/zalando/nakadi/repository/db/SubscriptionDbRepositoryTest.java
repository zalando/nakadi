package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.util.HashGenerator;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;

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
import static org.zalando.nakadi.utils.TestUtils.createRandomSubscriptions;

public class SubscriptionDbRepositoryTest extends AbstractDbRepositoryTest {

    private static final Comparator<Subscription> SUBSCRIPTION_CREATION_DATE_DESC_COMPARATOR =
            (sub1, sub2) -> sub2.getCreatedAt().compareTo(sub1.getCreatedAt());

    private SubscriptionDbRepository repository;
    private HashGenerator hashGenerator = new HashGenerator();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        repository = new SubscriptionDbRepository(
                template, TestUtils.OBJECT_MAPPER, new UUIDGenerator(), hashGenerator);
    }

    @Test
    public void whenCreateSubscriptionThenOk() throws Exception {

        final SubscriptionBase subscription = RandomSubscriptionBuilder.builder().build();
        final Subscription createdSubscription = repository.createSubscription(subscription);
        checkSubscriptionCreatedFromSubscriptionBase(createdSubscription, subscription);

        final int rows = template.queryForObject("SELECT count(*) FROM zn_data.subscription where s_id=?",
                Integer.class, createdSubscription.getId());
        assertThat("Number of rows should be 1", rows, equalTo(1));

        final Map<String, Object> result = template.queryForMap(
                "SELECT s_subscription_object FROM zn_data.subscription WHERE s_id=?", createdSubscription.getId());

        final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();
        final Subscription saved = mapper.readValue(result.get("s_subscription_object").toString(), Subscription.class);
        assertThat("Saved subscription equal to original one", saved, equalTo(createdSubscription));
    }

    @Test(expected = DuplicatedSubscriptionException.class)
    public void whenCreateSubscriptionWithDuplicatedKeyParamsThenDuplicatedSubscriptionException() throws Exception {

        final SubscriptionBase subscription = RandomSubscriptionBuilder.builder().build();
        repository.createSubscription(subscription);

        // try to create subscription second time
        repository.createSubscription(subscription);
    }

    @Test
    public void whenGetSubscriptionByIdThenOk() throws Exception {

        // insert subscription into DB
        final Subscription subscription = RandomSubscriptionBuilder.builder().build();
        insertSubscriptionToDB(subscription);

        // get subscription by id and compare to original
        final Subscription gotSubscription = repository.getSubscription(subscription.getId());
        assertThat("We found the needed subscription", gotSubscription, equalTo(subscription));
    }

    @Test
    public void whenGetSubscriptionByKeyPropertiesThenOk() throws Exception {
        final String owningApplication = TestUtils.randomUUID();
        // insert subscription into DB
        final Subscription subscription = RandomSubscriptionBuilder.builder()
                .withOwningApplication(owningApplication)
                .withEventTypes(ImmutableSet.of("my-et", "second-et"))
                .withConsumerGroup("my-cg")
                .build();
        insertSubscriptionToDB(subscription);

        // get subscription by key properties and compare to original
        final Subscription gotSubscription = repository.getSubscription(owningApplication,
                ImmutableSet.of("second-et", "my-et"),
                "my-cg");
        assertThat("We found the needed subscription", gotSubscription, equalTo(subscription));
    }

    @Test
    public void whenListSubscriptionsByOwningApplicationAndEventTypeThenOk()
            throws ServiceTemporarilyUnavailableException {

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
                .sorted(SUBSCRIPTION_CREATION_DATE_DESC_COMPARATOR)
                .collect(toList());

        final List<Subscription> subscriptions = repository.listSubscriptions(ImmutableSet.of("et1"),
                Optional.of(owningApp), 0, 10);
        assertThat(subscriptions, equalTo(expectedSubscriptions));
    }

    @Test
    public void whenListSubscriptionsByMultipleEventTypesThenOk() throws ServiceTemporarilyUnavailableException {
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
                .sorted(SUBSCRIPTION_CREATION_DATE_DESC_COMPARATOR)
                .collect(toList());

        final List<Subscription> subscriptions = repository.listSubscriptions(ImmutableSet.of(et1, et2),
                Optional.empty(), 0, 10);
        assertThat(subscriptions, equalTo(expectedSubscriptions));
    }

    @Test
    public void whenListSubscriptionsLimitAndOffsetAreRespected() throws ServiceTemporarilyUnavailableException {
        final String owningApp = TestUtils.randomUUID();
        final List<Subscription> testSubscriptions = createRandomSubscriptions(10, owningApp);
        testSubscriptions.forEach(this::insertSubscriptionToDB);

        testSubscriptions.sort(SUBSCRIPTION_CREATION_DATE_DESC_COMPARATOR);
        testSubscriptions.subList(0, 2).clear();
        testSubscriptions.subList(3, testSubscriptions.size()).clear();

        final List<Subscription> subscriptions = repository.listSubscriptions(emptySet(), Optional.of(owningApp), 2, 3);
        assertThat(subscriptions, equalTo(testSubscriptions));
    }

    @Test
    public void whenDeleteSubscriptionThenOk()
            throws ServiceTemporarilyUnavailableException, NoSuchSubscriptionException {
        final Subscription subscription = RandomSubscriptionBuilder.builder().build();
        insertSubscriptionToDB(subscription);

        repository.deleteSubscription(subscription.getId());

        final Integer count = template.queryForObject("SELECT count(*) FROM zn_data.subscription WHERE s_id = ?",
                Integer.class, subscription.getId());
        assertThat("Subscription is removed", count, equalTo(0));
    }

    @Test(expected = NoSuchSubscriptionException.class)
    public void whenDeleteNoneExistingConnectionThenNoSuchSubscriptionException()
            throws ServiceTemporarilyUnavailableException,
            NoSuchSubscriptionException {
        repository.deleteSubscription("some-dummy-id");
    }

    private void insertSubscriptionToDB(final Subscription subscription) {
        try {
            template.update("INSERT INTO zn_data.subscription (s_id, s_subscription_object, s_key_fields_hash) " +
                    "VALUES (?, ?::JSONB, ?)",
                    subscription.getId(),
                    TestUtils.OBJECT_MAPPER.writer().writeValueAsString(subscription),
                    hashGenerator.generateSubscriptionKeyFieldsHash(subscription));
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
        assertThat(subscription.getReadFrom(), equalTo(subscriptionBase.getReadFrom()));
    }

}
