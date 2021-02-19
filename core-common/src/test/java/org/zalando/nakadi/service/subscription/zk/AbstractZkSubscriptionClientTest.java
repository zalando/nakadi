package org.zalando.nakadi.service.subscription.zk;

import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.service.subscription.zk.lock.NakadiLock;

import java.util.UUID;

public class AbstractZkSubscriptionClientTest {

    @Test
    public void testShouldUnlockIfNotAcquired() {
        final String subscriptionId = UUID.randomUUID().toString();
        final NakadiLock nl = Mockito.mock(NakadiLock.class);
        final NewZkSubscriptionClient client = new NewZkSubscriptionClient(
                subscriptionId,
                null,
                nl,
                "log-path",
                null
        );

        Mockito.when(nl.lock()).thenReturn(false);
        try {
            client.runLocked(() -> "executed");
        } catch (final ServiceTemporarilyUnavailableException e) {
            // skip
        }

        Mockito.verify(nl, Mockito.times(1)).unlock();
    }

}