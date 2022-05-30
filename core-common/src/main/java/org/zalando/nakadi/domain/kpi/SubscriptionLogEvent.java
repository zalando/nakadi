package org.zalando.nakadi.domain.kpi;

import org.apache.avro.Schema;
import org.zalando.nakadi.config.KPIEventTypes;
import org.zalando.nakadi.util.AvroUtils;

import java.io.IOException;

public class SubscriptionLogEvent extends KPIEvent {

    private static final String PATH_SCHEMA =
            "event-type-schema/nakadi.subscription.log/nakadi.subscription.log.1.avsc";
    private static final Schema SCHEMA;

    static {
        // load latest local schema
        try {
            SCHEMA = AvroUtils.getParsedSchema(
                    KPIEvent.class.getClassLoader().getResourceAsStream(PATH_SCHEMA)
            );
        } catch (IOException e) {
            throw new RuntimeException("failed to load avro schema");
        }
    }

    @KPIField("subscription_id")
    protected String subscriptionId;
    @KPIField("status")
    protected String status;

    public SubscriptionLogEvent() {
        super(KPIEventTypes.SUBSCRIPTION_LOG);
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public SubscriptionLogEvent setSubscriptionId(final String subscriptionId) {
        this.subscriptionId = subscriptionId;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public SubscriptionLogEvent setStatus(final String status) {
        this.status = status;
        return this;
    }

    @Override
    public Schema getSchema() {
        return SCHEMA;
    }
}
