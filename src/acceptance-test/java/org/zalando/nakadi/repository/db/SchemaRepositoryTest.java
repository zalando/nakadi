package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import java.util.List;

public class SchemaRepositoryTest extends AbstractDbRepositoryTest {

    private SchemaRepository repository;

    public SchemaRepositoryTest() {
        super(new String[]{"zn_data.event_type_schema"});
    }

    @Before
    public void setUp() {
        super.setUp();
        repository = new SchemaRepository(template, mapper);
    }

    @Test
    public void whenListVersionsListedOrdered() throws Exception {
        final EventTypeSchemaBase schemaBase = new EventTypeSchemaBase(EventTypeSchemaBase.Type.JSON_SCHEMA, "schema");
        final EventType eventType = EventTypeTestBuilder.builder()
                .name("test_et_name_schemarepositorytest")
                .schema(new EventTypeSchema(schemaBase, "1.0.2", DateTime.now()))
                .build();
        insertEventType(eventType);
        insertSchema(eventType);
        eventType.setSchema(new EventTypeSchema(schemaBase, "2.10.3", DateTime.now()));
        insertSchema(eventType);
        eventType.setSchema(new EventTypeSchema(schemaBase, "10.0.0", DateTime.now()));
        insertSchema(eventType);

        final List<EventTypeSchema> schemas = repository.getSchemas("test_et_name_schemarepositorytest", 0, 3);
        Assert.assertEquals(3, schemas.size());
        Assert.assertEquals(new Version("10.0.0"), schemas.get(0).getVersion());
        Assert.assertEquals(new Version("2.10.3"), schemas.get(1).getVersion());
        Assert.assertEquals(new Version("1.0.2"), schemas.get(2).getVersion());

        int count = repository.getSchemasCount("test_et_name_schemarepositorytest");
        Assert.assertEquals(3, count);
    }

    private void insertSchema(final EventType eventType) throws JsonProcessingException {
        template.update(
                "INSERT INTO zn_data.event_type_schema (ets_event_type_name, ets_schema_object) VALUES (?, ?::jsonb)",
                eventType.getName(),
                mapper.writer().writeValueAsString(eventType.getSchema()));
    }

    private void insertEventType(final EventType eventType) throws Exception {
        final String insertSQL = "INSERT INTO zn_data.event_type (et_name, et_topic, et_event_type_object) " +
                "VALUES (?, ?, to_json(?::json))";
        template.update(insertSQL,
                eventType.getName(),
                eventType.getTopic(),
                mapper.writer().writeValueAsString(eventType));
    }

}