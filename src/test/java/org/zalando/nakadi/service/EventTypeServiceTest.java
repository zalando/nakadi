package org.zalando.nakadi.service;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.validation.SchemaEvolutionService;

import java.util.Optional;

public class EventTypeServiceTest {

    private final EventTypeRepository eventTypeRepository = Mockito.mock(EventTypeRepository.class);
    private final TopicRepository topicRepository = Mockito.mock(TopicRepository.class);
    private final PartitionResolver partitionResolver = Mockito.mock(PartitionResolver.class);
    private final Enrichment enrichment = Mockito.mock(Enrichment.class);
    private final FeatureToggleService featureToggleService = Mockito.mock(FeatureToggleService.class);
    private final SubscriptionDbRepository subscriptionRepository = Mockito.mock(SubscriptionDbRepository.class);
    private final SchemaEvolutionService schemaEvolutionService = Mockito.mock(SchemaEvolutionService.class);
    private final EventTypeService eventTypeService = new EventTypeService(eventTypeRepository, topicRepository,
            partitionResolver, enrichment, new UUIDGenerator(), featureToggleService, subscriptionRepository,
            schemaEvolutionService);

    @Test
    public void testValidateSchemaEndingBracket() {
        final EventType eventType = EventTypeTestBuilder.builder()
                .schema("{\"additionalProperties\": true}}")
                .build();
        final Result<Void> result = eventTypeService.create(eventType);
        Assert.assertFalse(result.isSuccessful());
        Assert.assertEquals(Optional.of("schema must be a valid json"), result.getProblem().getDetail());
    }

    @Test
    public void testValidateSchemaMultipleRoots() {
        final EventType eventType = EventTypeTestBuilder.builder()
                .schema("{\"additionalProperties\": true}{\"additionalProperties\": true}")
                .build();
        final Result<Void> result = eventTypeService.create(eventType);
        Assert.assertFalse(result.isSuccessful());
        Assert.assertEquals(Optional.of("schema must be a valid json"), result.getProblem().getDetail());
    }

    @Test
    public void testValidateSchemaArbitraryEndingTest() {
        final EventType eventType = EventTypeTestBuilder.builder()
                .schema("{\"additionalProperties\": true}NakadiRocks")
                .build();
        final Result<Void> result = eventTypeService.create(eventType);
        Assert.assertFalse(result.isSuccessful());
        Assert.assertEquals(Optional.of("schema must be a valid json"), result.getProblem().getDetail());
    }

    @Test
    public void testValidateSchemaArrayEndingTest() {
        final EventType eventType = EventTypeTestBuilder.builder()
                .schema("[{\"additionalProperties\": true}]]")
                .build();
        final Result<Void> result = eventTypeService.create(eventType);
        Assert.assertFalse(result.isSuccessful());
        Assert.assertEquals(Optional.of("schema must be a valid json"), result.getProblem().getDetail());
    }

    @Test
    public void testValidateSchemaFormattedJsonSchema() {
        final EventType eventType = EventTypeTestBuilder.builder()
                .schema("{\n" +
                        "properties:{\n" +
                        "event:{\n" +
                        "type:\"string\"\n" +
                        "},\n" +
                        "topic:{\n" +
                        "type:\"string\"\n" +
                        "},\n" +
                        "meta_data:{\n" +
                        "$ref:\"#/definitions/metadata\"\n" +
                        "},\n" +
                        "body:{\n" +
                        "$ref:\"#/definitions/tote_state\"\n" +
                        "}\n" +
                        "},\n" +
                        "definitions:{\n" +
                        "metadata:{\n" +
                        "properties:{\n" +
                        "occured_at:{\n" +
                        "type:\"string\"\n" +
                        "},\n" +
                        "eid:{\n" +
                        "type:\"string\"\n" +
                        "}\n" +
                        "},\n" +
                        "type:\"object\"\n" +
                        "},\n" +
                        "tote_state:{\n" +
                        "properties:{\n" +
                        "barcode:{\n" +
                        "type:\"string\"\n" +
                        "},\n" +
                        "state:{\n" +
                        "enum:[\n" +
                        "\"/status/closed\"\n" +
                        "]\n" +
                        "}\n" +
                        "},\n" +
                        "type:\"object\"\n" +
                        "}\n" +
                        "},\n" +
                        "type:\"object\"\n" +
                        "}")
                .build();
        final Result<Void> result = eventTypeService.create(eventType);
        Assert.assertTrue(result.isSuccessful());
    }

}