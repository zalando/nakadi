package de.zalando.aruha.nakadi.repository.db;

import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.DuplicatedSubscriptionException;
import org.junit.Before;
import org.junit.Test;

import static java.util.UUID.randomUUID;

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
