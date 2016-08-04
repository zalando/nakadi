package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.zalando.nakadi.utils.RandomSubscriptionBuilder.randomSubscription;

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

        final List<Subscription> testSubscriptions = ImmutableList.of(
                randomSubscription().build(), randomSubscription().build())
                .stream()
                .sorted(SUBSCRIPTION_CREATION_DATE_DESC_COMPARATOR)
                .collect(toList());

        testSubscriptions.forEach(this::insertSubscriptionToDB);

        final List<Subscription> subscriptions = repository.listSubscriptions();
        assertThat(subscriptions, equalTo(testSubscriptions));
    }

    @Test
    public void whenListSubscriptionsByOwningApplicationThenOk() throws ServiceUnavailableException {

        final List<Subscription> testSubscriptions = ImmutableList.of(
                randomSubscription().withOwningApplication("myapp1").build(),
                randomSubscription().withOwningApplication("myapp1").build(),
                randomSubscription().withOwningApplication("myapp2").build());

        testSubscriptions.forEach(this::insertSubscriptionToDB);

        final List<Subscription> expectedSubscriptions = testSubscriptions.stream()
                .filter(sub -> "myapp1".equals(sub.getOwningApplication()))
                .sorted(SUBSCRIPTION_CREATION_DATE_DESC_COMPARATOR)
                .collect(toList());

        final List<Subscription> subscriptions = repository.listSubscriptionsForOwningApplication("myapp1");
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
