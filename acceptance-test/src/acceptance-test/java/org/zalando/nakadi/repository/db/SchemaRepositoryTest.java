package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.TestUtils;

import java.util.List;

import static org.zalando.nakadi.utils.TestUtils.randomUUID;

public class SchemaRepositoryTest extends AbstractDbRepositoryTest {

    private SchemaRepository repository;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        repository = new SchemaRepository(template, TestUtils.OBJECT_MAPPER);
    }

    @Test
    public void whenListVersionsListedOrdered() throws Exception {
        final String name = randomUUID();
        buildEventWithMultipleSchemas(name);

        final List<EventTypeSchema> schemas = repository.getSchemas(name, 0, 3);
        Assert.assertEquals(3, schemas.size());
        Assert.assertEquals("10.0.0", schemas.get(0).getVersion());
        Assert.assertEquals("2.10.3", schemas.get(1).getVersion());
        Assert.assertEquals("1.0.2", schemas.get(2).getVersion());

        final int count = repository.getSchemasCount(name);
        Assert.assertEquals(3, count);
    }

    @Test
    public void whenGetLatestSchemaReturnLatest() throws Exception {
        final String name = randomUUID();
        buildEventWithMultipleSchemas(name);
        final EventTypeSchema schema = repository.getSchemaVersion(name, "10.0.0");
        Assert.assertEquals("10.0.0", schema.getVersion());
        Assert.assertEquals("schema", schema.getSchema());
    }

    @Test
    public void whenGetOldSchemaReturnOld() throws Exception {
        final String name = randomUUID();
        buildEventWithMultipleSchemas(name);
        final EventTypeSchema schema = repository.getSchemaVersion(name, "1.0.2");
        Assert.assertEquals("1.0.2", schema.getVersion());
        Assert.assertEquals("schema", schema.getSchema());
    }

    private void buildEventWithMultipleSchemas(final String name) throws Exception {
        final EventTypeSchemaBase schemaBase = new EventTypeSchemaBase(EventTypeSchemaBase.Type.JSON_SCHEMA, "schema");
        final EventType eventType = EventTypeTestBuilder.builder()
                .name(name)
                .build();
        insertEventType(eventType);
        eventType.setSchema(new EventTypeSchema(schemaBase, "1.0.2", DateTime.parse("2016-12-07T10:44:16.378+01:00")));
        insertSchema(eventType);
        eventType.setSchema(new EventTypeSchema(schemaBase, "2.10.3", DateTime.parse("2016-12-07T10:54:16.778+01:00")));
        insertSchema(eventType);
        eventType.setSchema(new EventTypeSchema(schemaBase, "10.0.0", DateTime.parse("2016-12-07T10:57:16.200+01:00")));
        insertSchema(eventType);
    }

    private void insertSchema(final EventType eventType) throws JsonProcessingException {
        template.update(
                "INSERT INTO zn_data.event_type_schema (ets_event_type_name, ets_schema_object) VALUES (?, ?::jsonb)",
                eventType.getName(),
                TestUtils.OBJECT_MAPPER.writer().writeValueAsString(eventType.getSchema()));
    }

    private void insertEventType(final EventType eventType) throws Exception {
        final String insertSQL = "INSERT INTO zn_data.event_type (et_name, et_event_type_object) " +
                "VALUES (?, to_json(?::json))";
        template.update(insertSQL,
                eventType.getName(),
                TestUtils.OBJECT_MAPPER.writer().writeValueAsString(eventType));
    }

}
