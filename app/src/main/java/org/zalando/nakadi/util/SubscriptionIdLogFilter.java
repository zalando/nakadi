package org.zalando.nakadi.util;

import org.apache.log4j.Priority;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

import java.util.HashSet;
import java.util.Set;

public class SubscriptionIdLogFilter extends Filter {
    private Priority threshold;
    private String subscriptionIds;
    private final Set<String> subscriptionIdsSet = new HashSet<>();

    public Priority getThreshold() {
        return threshold;
    }

    public void setThreshold(final Priority threshold) {
        this.threshold = threshold;
    }

    public String getSubscriptionIds() {
        return subscriptionIds;
    }

    public void setSubscriptionIds(final String subscriptionIds) {
        this.subscriptionIds = subscriptionIds;
        this.subscriptionIdsSet.clear();
        for (final String item : this.subscriptionIds.split("[,;]")) {
            final String subscriptionId = item.strip();
            if (subscriptionId.isEmpty()) {
                continue;
            }
            this.subscriptionIdsSet.add(subscriptionId);
        }
    }

    @Override
    public int decide(final LoggingEvent event) {
        if (threshold != null && event.getLevel().isGreaterOrEqual(threshold)) {
            return Filter.ACCEPT;
        }
        final Object subscriptionId = event.getMDC("subscriptionId");
        if (subscriptionId == null) {
            // If subscription id is not set - log it, as probably it is some related code, that was not covered by
            // proper MDC (Find this code and fix it)
            return Filter.NEUTRAL;
        }
        if (subscriptionIdsSet.contains(String.valueOf(subscriptionId))) {
            // Subscription id is in the list of ids to log, one need to log it
            return Filter.NEUTRAL;
        }
        // Subscription id is not in the list, will not log
        return Filter.DENY;
    }
}
