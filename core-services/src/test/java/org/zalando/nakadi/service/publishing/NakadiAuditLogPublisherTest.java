package org.zalando.nakadi.service.publishing;

import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.security.UsernameHasher;
import org.zalando.nakadi.service.FeatureToggleService;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public class NakadiAuditLogPublisherTest {

    public class TestSubject implements Subject {
        String name;

        public TestSubject(final String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    @Test
    public void testPublishAuditLog() {
        final JsonEventProcessor eventsProcessor = mock(JsonEventProcessor.class);
        final FeatureToggleService toggle = mock(FeatureToggleService.class);
        final AuthorizationService authorizationService = mock(AuthorizationService.class);

        when(toggle.isFeatureEnabled(Feature.AUDIT_LOG_COLLECTION)).thenReturn(true);

        final NakadiAuditLogPublisher publisher = new NakadiAuditLogPublisher(
                toggle,
                eventsProcessor,
                new EventMetadataTestStub(),
                new JsonConfig().jacksonObjectMapper(),
                new UsernameHasher("salt"),
                authorizationService,
                "audit-event-type");
        when(authorizationService.getSubject()).thenReturn(Optional.of(new TestSubject("user-name")));
        final DateTime now = DateTime.parse("2019-01-16T13:44:16.819Z");
        final EventType et = buildDefaultEventType();
        et.setName("new-et-name");
        et.setCreatedAt(now);
        et.setUpdatedAt(now);
        et.getSchema().setCreatedAt(now);
        publisher.publish(Optional.empty(), Optional.of(et),
                NakadiAuditLogPublisher.ResourceType.EVENT_TYPE,
                NakadiAuditLogPublisher.ActionType.CREATED, "et-name");

        final ArgumentCaptor<List<JSONObject>> supplierCaptor = ArgumentCaptor.forClass(List.class);
        verify(eventsProcessor, times(1)).sendEvents(
                eq("audit-event-type"),
                supplierCaptor.capture());

        Assert.assertEquals(new JSONArray("[{\"data_op\":\"C\",\"data\":{\"new_object\":{\"schema\":" +
                        "{\"schema\":\"{ \\\"properties\\\": { \\\"foo\\\": { \\\"type\\\": \\\"string\\\" " +
                        "} } }\",\"created_at\":\"2019-01-16T13:44:16.819Z\",\"type\":\"json_schema\"," +
                        "\"version\":\"1.0.0\"},\"compatibility_mode\":\"compatible\",\"ordering_key_fields\":[]," +
                        "\"annotations\":{}," +
                        "\"created_at\":\"2019-01-16T13:44:16.819Z\",\"cleanup_policy\":\"delete\"," +
                        "\"ordering_instance_ids\":[],\"authorization\":null,\"labels\":{}," +
                        "\"partition_key_fields\":[]," +
                        "\"updated_at\":\"2019-01-16T13:44:16.819Z\"," +
                        "\"default_statistic\":{\"read_parallelism\":1," +
                        "\"messages_per_minute\":1,\"message_size\":1,\"write_parallelism\":1}," +
                        "\"name\":\"new-et-name\",\"options\":{\"retention_time\":172800000}," +
                        "\"partition_strategy\":\"random\",\"owning_application\":\"event-producer-application\"," +
                        "\"enrichment_strategies\":[],\"category\":\"undefined\"}," +
                        "\"new_text\":\"{\\\"name\\\":\\\"new-et-name\\\"," +
                        "\\\"owning_application\\\":\\\"event-producer-application\\\"," +
                        "\\\"category\\\":\\\"undefined\\\",\\\"enrichment_strategies\\\":[]," +
                        "\\\"partition_strategy\\\":\\\"random\\\",\\\"partition_key_fields\\\":[]," +
                        "\\\"cleanup_policy\\\":\\\"delete\\\",\\\"ordering_key_fields\\\":[]," +
                        "\\\"ordering_instance_ids\\\":[],\\\"schema\\\":{\\\"type\\\":\\\"json_schema\\\"," +
                        "\\\"schema\\\":\\\"{ \\\\\\\"properties\\\\\\\": { \\\\\\\"foo\\\\\\\": " +
                        "{ \\\\\\\"type\\\\\\\": \\\\\\\"string\\\\\\\" } } }\\\",\\\"version\\\":\\\"1.0.0\\\"," +
                        "\\\"created_at\\\":\\\"2019-01-16T13:44:16.819Z\\\"}," +
                        "\\\"default_statistic\\\":{\\\"messages_per_minute\\\":1,\\\"message_size\\\":1," +
                        "\\\"read_parallelism\\\":1,\\\"write_parallelism\\\":1}," +
                        "\\\"options\\\":{\\\"retention_time\\\":172800000}," +
                        "\\\"authorization\\\":null,\\\"annotations\\\":{},\\\"labels\\\":{}," +
                        "\\\"compatibility_mode\\\":\\\"compatible\\\"," +
                        "\\\"updated_at\\\":\\\"2019-01-16T13:44:16.819Z\\\"," +
                        "\\\"created_at\\\":\\\"2019-01-16T13:44:16.819Z\\\"}\"," +
                        "\"user_hash\":\"89bc5f7398509d3ce86c013c138e11357ff7f589fca9d58cfce443c27f81956c\"," +
                        "\"resource_type\":\"event_type\",\"resource_id\":\"et-name\",\"user\":\"user-name\"}," +
                        "\"data_type\":\"event_type\"}]\n").toString(),
                new JSONArray(supplierCaptor.getValue()).toString());
    }

}