package org.zalando.nakadi.util;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.SubscriptionBase;

import static com.google.common.base.Charsets.UTF_8;

@Component
public class HashGenerator {

    public String generateSubscriptionKeyFieldsHash(final SubscriptionBase subscription) {
        final Hasher hasher = Hashing.md5()
                .newHasher()
                .putString(subscription.getOwningApplication(), UTF_8)
                .putString(subscription.getConsumerGroup(), UTF_8);
        subscription.getEventTypes()
                .stream()
                .sorted()
                .forEach(et -> hasher.putString(et, UTF_8));
        return hasher.hash().toString();
    }

}
