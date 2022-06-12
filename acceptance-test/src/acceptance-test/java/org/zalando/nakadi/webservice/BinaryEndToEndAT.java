package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Ignore;
import org.junit.Test;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import java.util.Base64;
import java.util.List;

import static com.jayway.restassured.RestAssured.given;

public class BinaryEndToEndAT extends BaseAT {
    private static final String TEST_ET_NAME = "nakadi.test-2022-05-06.et";
    private static final String TEST_DATA = "BAAAAGO0v/qJk2BIZjU5ZmVlNTQtMmNhYy00MTAzLWI4NTItOGMwOGRiZjhlNjEyAhJ0ZX" +
            "N0LWZsb3cAAjECEnRlc3QtdXNlcjJuYWthZGkudGVzdC0yMDIyLTA1LTA2LmV0AAAAAAAAAABBCFBPU1QYL2V2ZW50LXR5cGVzAB50ZX" +
            "N0LXVzZXItYWdlbnQMbmFrYWRpFGhhc2hlZC1hcHCSAxQCLQQtLfYBggU=";

    private static final String TEST_EVENT_WITHOUT_PAYLOAD = "BQAAAHDYldi7019IMzJmNWRhZTUtNGZjNC00Y2RhLWJlMDctYjMxM2" +
            "I1ODQ5MGFiAgZoZWsC2JXYu9NfAjECFm5ha2FkaS10ZXN0QnRlc3QtZXQtZm9yLWRlbGV0ZS1jbGVhbnVwLXBvbGljeQICM" +
            "AAAAAAAAAAAAA==";

    private static final String TEST_COMPACTED_EVENT_WITH_PAYLOAD = "BQAAAGnYldi7019IMzJmNWRhZTUtNGZjNC00Y2RhLWJlMDct" +
            "YjMxM2I1ODQ5MGFiAgZoZWsC2JXYu9NfAjECFm5ha2FkaS10ZXN0NHRlc3QtZXQtZm9yLWRlbGV0ZS1wYXlsb2FkAgIwAAAAAAAAAAAt" +
            "CFBPU1QYL2V2ZW50LXR5cGVzAAxuYWthZGkUaGFzaGVkLWFwcJIDFAItBC0t";

    private static final String TEST_COMPACTED_EVENT_WITHOUT_PAYLOAD = "BQAAAGzYldi7019IMzJmNWRhZTUtNGZjNC00Y2RhLWJlM" +
            "DctYjMxM2I1ODQ5MGFiAgZoZWsC2JXYu9NfAjECFm5ha2FkaS10ZXN0OnRlc3QtZXQtZm9yLXN1Y2Nlc3NmdWwtZGVsZXRlAgIwAAAAA" +
            "AAAAAAA";

    @Test
    @Ignore
    public void testAvroPublishing() throws JsonProcessingException {
        final var et = EventTypeTestBuilder.builder()
                .name(TEST_ET_NAME)
                .category(EventCategory.BUSINESS)
                .enrichmentStrategies(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(et);

        final byte[] body = Base64.getDecoder().decode(TEST_DATA);

        final var response = given()
                .contentType("application/avro-binary; charset=utf-8")
                .body(body)
                .post(String.format("/event-types/%s/events", TEST_ET_NAME));
        response.print();
        response.then().statusCode(200);
        // TODO add the consumption side once schema creation is done.
    }

    @Test
    public void testAvroDeleteCannotWorkWhenCleanupPolicyIsDelete() throws JsonProcessingException {
        final var etName = "test-et-for-delete-cleanup-policy";
        final var et = EventTypeTestBuilder.builder()
                .name(etName)
                .category(EventCategory.BUSINESS)
                .cleanupPolicy(CleanupPolicy.DELETE)
                .enrichmentStrategies(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(et);

        final byte[] body = Base64.getDecoder().decode(TEST_EVENT_WITHOUT_PAYLOAD);

        final var response = given()
                .contentType("application/avro-binary; charset=utf-8")
                .body(body)
                .post(String.format("/event-types/%s/deleted-events", etName));
        response.print();
        response.then()
                .statusCode(422);
    }

    @Test
    public void testAvroDeleteCannotWorkWithPayload() throws JsonProcessingException {
        final var etName = "test-et-for-delete-payload";
        final var et = EventTypeTestBuilder.builder()
                .name(etName)
                .category(EventCategory.BUSINESS)
                .cleanupPolicy(CleanupPolicy.COMPACT)
                .enrichmentStrategies(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(et);

        final byte[] body = Base64.getDecoder().decode(TEST_COMPACTED_EVENT_WITH_PAYLOAD);

        final var response = given()
                .contentType("application/avro-binary; charset=utf-8")
                .body(body)
                .post(String.format("/event-types/%s/deleted-events", etName));
        response.print();
        response.then()
                .statusCode(207);
    }

    @Test
    @Ignore
    public void testAvroDelete() throws JsonProcessingException {
        final var etName = "test-et-for-successful-delete";
        final var et = EventTypeTestBuilder.builder()
                .name(etName)
                .category(EventCategory.BUSINESS)
                .cleanupPolicy(CleanupPolicy.COMPACT)
                .enrichmentStrategies(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(et);

        final byte[] body = Base64.getDecoder().decode(TEST_COMPACTED_EVENT_WITHOUT_PAYLOAD);

        final var response = given()
                .contentType("application/avro-binary; charset=utf-8")
                .body(body)
                .post(String.format("/event-types/%s/deleted-events", etName));
        response.print();
        response.then()
                .statusCode(200);
    }
}
