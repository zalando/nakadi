package de.zalando.aruha.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.DuplicatedSubscriptionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class SubscriptionDbRepositoryTest extends AbstractDbRepositoryTest {

    private SubscriptionDbRepository repository;

    public SubscriptionDbRepositoryTest() {
        super("zn_data.subscription");
    }

    @Before
    public void setUp() {
        super.setUp();
        repository = new SubscriptionDbRepository(template, mapper);
    }

    @Test
    public void whenCreateSubscriptionThenOk() throws Exception {

        final Subscription subscription = createSubscription();
        repository.saveSubscription(subscription);

        final int rows = template.queryForObject("SELECT count(*) FROM zn_data.subscription", Integer.class);
        assertThat("Number of rows should be 1", rows, equalTo(1));

        final Map<String, Object> result =
                template.queryForMap("SELECT s_id, s_subscription_object FROM zn_data.subscription");
        assertThat("Id is persisted", result.get("s_id"), equalTo(subscription.getId()));

        final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();
        final Subscription saved = mapper.readValue(result.get("s_subscription_object").toString(), Subscription.class);
        assertThat("Saved subscription equal to original one", saved, equalTo(subscription));
    }

    @Test(expected = DuplicatedSubscriptionException.class)
    public void whenCreateSubscriptionWithDuplicatedKeyParamsThenDuplicatedSubscriptionException() throws Exception {

        final Subscription subscription = createSubscription();
        repository.saveSubscription(subscription);

        // set another id to subscription but keep the same key properties
        subscription.setId(randomUUID().toString());
        repository.saveSubscription(subscription);
    }

    private Subscription createSubscription() {
        final Subscription subscription = new Subscription();
        subscription.setId(randomUUID().toString());
        subscription.setOwningApplication("my_consumer");
        subscription.setEventTypes(ImmutableList.of("my_et"));
        return subscription;
    }

}
