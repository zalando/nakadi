package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.util.UUIDGenerator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;

public class SubscriptionDbRepositoryTest extends AbstractDbRepositoryTest {

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

        final SubscriptionBase subscription = createSubscriptionBase();
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

        final SubscriptionBase subscription = createSubscriptionBase();
        repository.createSubscription(subscription);

        // try to create subscription second time
        repository.createSubscription(subscription);
    }

    @Test
    public void whenGetSubscriptionByIdThenOk() throws InternalNakadiException, JsonProcessingException,
            NoSuchSubscriptionException {

        // insert subscription into DB
        final Subscription subscription = createSubscription();
        insertSubscriptionToDB(subscription);

        // get subscription by id and compare to original
        final Subscription gotSubscription = repository.getSubscription(subscription.getId());
        assertThat("We found the needed subscription", gotSubscription, equalTo(subscription));
    }

    @Test
    public void whenGetSubscriptionByKeyPropertiesThenOk() throws InternalNakadiException, JsonProcessingException,
            NoSuchSubscriptionException {

        // insert subscription into DB
        final Subscription subscription = createSubscription("myapp", ImmutableSet.of("my-et", "second-et"), "my-cg");
        insertSubscriptionToDB(subscription);

        // get subscription by key properties and compare to original
        final Subscription gotSubscription = repository.getSubscription("myapp", ImmutableSet.of("second-et", "my-et"),
                "my-cg");
        assertThat("We found the needed subscription", gotSubscription, equalTo(subscription));
    }

    private void insertSubscriptionToDB(final Subscription subscription) throws JsonProcessingException {
        template.update("INSERT INTO zn_data.subscription (s_id, s_subscription_object) VALUES (?, ?::jsonb)",
                subscription.getId(), mapper.writer().writeValueAsString(subscription));
    }

    private Subscription createSubscription() {
        final Subscription subscription = new Subscription();
        subscription.setId(randomUUID().toString());
        subscription.setOwningApplication("myapp");
        subscription.setEventTypes(ImmutableSet.of("my-et"));
        subscription.setCreatedAt(new DateTime(DateTimeZone.UTC));
        return subscription;
    }

    private SubscriptionBase createSubscriptionBase() {
        final Subscription subscription = new Subscription();
        subscription.setOwningApplication("myapp");
        subscription.setEventTypes(ImmutableSet.of("my-et"));
        return subscription;
    }

    private Subscription createSubscription(final String owningApplication, final Set<String> eventTypes,
                                            final String consumerGroup) {
        final Subscription subscription = createSubscription();
        subscription.setOwningApplication(owningApplication);
        subscription.setEventTypes(eventTypes);
        subscription.setConsumerGroup(consumerGroup);
        return subscription;
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
