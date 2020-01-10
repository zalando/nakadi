package org.zalando.nakadi.util;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class HashGeneratorTest {

    private final HashGenerator hashGenerator;

    public HashGeneratorTest() {
        hashGenerator = new HashGenerator();
    }

    @Test
    public void testActualHashImplementationIsNotChanged() {
        final SubscriptionBase subscription = createSubscription("my-app", "my-consumer-group", "et1", "et2");
        assertThat(
                hashGenerator.generateSubscriptionKeyFieldsHash(subscription),
                equalTo("a2749954511a4ff3423fe4cefd76b011"));
    }

    @Test
    public void whenGenerateHashForEqualSubscriptionsThenHashIsEqual() {
        final SubscriptionBase s1 = createSubscription("my-app", "abc", "et1", "et2");
        s1.setReadFrom(SubscriptionBase.InitialPosition.BEGIN);

        final SubscriptionBase s2 = createSubscription("my-app", "abc", "et2", "et1");
        s2.setReadFrom(SubscriptionBase.InitialPosition.CURSORS);
        s2.setInitialCursors(ImmutableList.of(new SubscriptionCursorWithoutToken("et1", "p1", "0")));

        assertThat(
                hashGenerator.generateSubscriptionKeyFieldsHash(s1),
                equalTo(hashGenerator.generateSubscriptionKeyFieldsHash(s2)));
    }

    @Test
    public void whenGenerateHashForNonEqualSubscriptionsThenHashIsDifferent() {
        checkHashesAreDifferent(
                createSubscription("my-app", "abc", "et1", "et2"),
                createSubscription("my-app", "abc", "et1", "et2", "et3"));

        checkHashesAreDifferent(
                createSubscription("my-app", "abc", "et1", "et2"),
                createSubscription("my-app-2", "abc", "et1", "et2"));

        checkHashesAreDifferent(
                createSubscription("my-app", "abc", "et1", "et2"),
                createSubscription("my-app", "abd", "et1", "et2"));
    }

    private void checkHashesAreDifferent(final SubscriptionBase s1, final SubscriptionBase s2) {
        assertThat(
                hashGenerator.generateSubscriptionKeyFieldsHash(s1),
                not(equalTo(hashGenerator.generateSubscriptionKeyFieldsHash(s2))));
    }

    private SubscriptionBase createSubscription(final String owningApp, final String consumerGroup,
                                                final String... eventTypes) {
        final SubscriptionBase subscription = new SubscriptionBase();
        subscription.setOwningApplication(owningApp);
        subscription.setConsumerGroup(consumerGroup);
        subscription.setEventTypes(newHashSet(eventTypes));
        return subscription;
    }

}
