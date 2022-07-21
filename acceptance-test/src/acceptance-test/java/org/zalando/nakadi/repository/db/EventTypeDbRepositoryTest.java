package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.domain.ResourceAuthorizationAttribute;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.utils.TestUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.randomTextString;

public class EventTypeDbRepositoryTest extends AbstractDbRepositoryTest {

    private EventTypeRepository repository;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        repository = new EventTypeRepository(template, TestUtils.OBJECT_MAPPER);
    }

    @Test
    public void whenCreateNewEventTypePersistItInTheDatabase() throws Exception {
        final EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);

        final SqlRowSet rs =
                template.queryForRowSet("SELECT et_event_type_object FROM zn_data.event_type WHERE et_name=?",
                        eventType.getName());
        rs.next();

        final EventType persisted = TestUtils.OBJECT_MAPPER.readValue(rs.getString(1), EventType.class);

        assertThat(persisted.getCategory(), equalTo(eventType.getCategory()));
        assertThat(persisted.getName(), equalTo(eventType.getName()));
        assertThat(persisted.getSchema().getType(), equalTo(eventType.getSchema().getType()));
        assertThat(persisted.getSchema().getSchema(), equalTo(eventType.getSchema().getSchema()));
    }

    @Test
    public void whenCreateNewEventTypeAlsoInsertIntoSchemaTable() throws Exception {
        final EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);

        final int rows = template.queryForObject(
                "SELECT count(*) FROM zn_data.event_type_schema where ets_event_type_name=?",
                Integer.class, eventType.getName());
        assertThat("Number of rows should increase", rows, equalTo(1));

        final SqlRowSet rs = template.queryForRowSet(
                "SELECT ets_schema_object FROM zn_data.event_type_schema where ets_event_type_name=?",
                eventType.getName());
        rs.next();

        final EventTypeSchema persisted = TestUtils.OBJECT_MAPPER.readValue(rs.getString(1), EventTypeSchema.class);

        assertThat(persisted.getVersion(), equalTo(eventType.getSchema().getVersion()));
        assertThat(persisted.getCreatedAt(), notNullValue());
        assertThat(persisted.getSchema(), equalTo(eventType.getSchema().getSchema()));
        assertThat(persisted.getType(), equalTo(eventType.getSchema().getType()));
    }

    @Test(expected = DuplicatedEventTypeNameException.class)
    public void whenCreateDuplicatedNamesThrowAnError() throws Exception {
        final EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);
        repository.saveEventType(eventType);
    }

    @Test
    public void whenEventExistsFindByNameReturnsSomething() throws Exception {
        final EventType eventType1 = buildDefaultEventType();
        final EventType eventType2 = buildDefaultEventType();

        insertEventType(eventType1);
        insertEventType(eventType2);

        final EventType persistedEventType = repository.findByName(eventType2.getName());

        assertThat(persistedEventType, notNullValue());
    }

    @Test
    public void whenEventTypeExistsFindByAuthorizationReturnsSomething() throws Exception {
        final EventType eventType1 = buildDefaultEventType();
        final ResourceAuthorizationAttribute auth = new ResourceAuthorizationAttribute(
                "service", "stups_test" + randomTextString());

        eventType1.setAuthorization(new ResourceAuthorization(
                Collections.emptyList(),
                Collections.emptyList(),
                List.of(auth)
        ));

        insertEventType(eventType1);

        final List<EventType> persistedEventTypes = repository.list(Optional.ofNullable(auth), Optional.empty());

        assertThat(persistedEventTypes, hasItem(hasProperty("name", is(eventType1.getName()))));
    }

    @Test
    public void whenEventTypeExistsFindByOwningApplication() throws Exception {
        final EventType eventType1 = buildDefaultEventType();
        final String owningApp = "some_owning_application";
        eventType1.setOwningApplication(owningApp);

        insertEventType(eventType1);

        final List<EventType> persistedEventTypes = repository.list(Optional.empty(), Optional.ofNullable(owningApp));

        assertThat(persistedEventTypes, hasItem(hasProperty("name", is(eventType1.getName()))));
    }

    @Test(expected = NoSuchEventTypeException.class)
    public void whenEventDoesntExistsFindByNameReturnsNothing() {
        repository.findByName("inexisting-name");
    }

    @Test
    public void whenUpdateExistingEventTypeItUpdates() throws IOException {
        final EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);

        eventType.setCategory(EventCategory.BUSINESS);
        eventType.setOwningApplication("new-application");

        repository.update(eventType);

        final int rows = template.queryForObject(
                "SELECT count(*) FROM zn_data.event_type_schema WHERE ets_event_type_name=?",
                Integer.class, eventType.getName());
        assertThat("Number of rows should increase", rows, equalTo(1));

        final SqlRowSet rs = template.queryForRowSet(
                "SELECT et_event_type_object FROM zn_data.event_type WHERE et_name=?", eventType.getName());
        rs.next();

        final EventType persisted = TestUtils.OBJECT_MAPPER.readValue(rs.getString(1), EventType.class);

        assertThat(persisted.getCategory(), equalTo(eventType.getCategory()));
        assertThat(persisted.getOwningApplication(), equalTo(eventType.getOwningApplication()));
        assertThat(persisted.getPartitionKeyFields(), equalTo(eventType.getPartitionKeyFields()));
        assertThat(persisted.getName(), equalTo(eventType.getName()));
        assertThat(persisted.getSchema().getType(), equalTo(eventType.getSchema().getType()));
        assertThat(persisted.getSchema().getSchema(), equalTo(eventType.getSchema().getSchema()));
    }

    @Test
    public void whenUpdateDifferentSchemaVersionThenInsertIt() throws IOException {
        final EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);

        eventType.getSchema().setVersion("1.1.0");

        repository.update(eventType);

        final int rows = template.queryForObject(
                "SELECT count(*) FROM zn_data.event_type_schema where ets_event_type_name=?",
                Integer.class, eventType.getName());
        assertThat("Number of rows should increase", rows, equalTo(2));
    }

    @Test
    public void whenListExistingEventTypesAreListed() {
        final EventType eventType1 = buildDefaultEventType();
        final EventType eventType2 = buildDefaultEventType();

        repository.saveEventType(eventType1);
        repository.saveEventType(eventType2);

        final List<EventType> eventTypes = repository.list(Optional.empty(), Optional.empty()).stream()
                .filter(et -> et.getName() != null)
                .filter(et -> et.getName().equals(eventType1.getName()) || et.getName().equals(eventType2.getName()))
                .collect(Collectors.toList());

        assertThat(eventTypes, hasSize(2));
    }

    @Test
    public void whenRemoveThenDeleteFromDatabase() throws Exception {
        final EventType eventType = buildDefaultEventType();

        insertEventType(eventType);

        int rows = template.queryForObject("SELECT count(*) FROM zn_data.event_type where et_name=?", Integer.class,
                eventType.getName());
        assertThat("After inserting event type it is present in db", rows, equalTo(1));

        repository.removeEventType(eventType.getName());

        rows = template.queryForObject("SELECT count(*) FROM zn_data.event_type where et_name=?", Integer.class,
                eventType.getName());
        assertThat("After deleting event type it is not present in db", rows, equalTo(0));
    }

    @Test
    public void unknownAttributesAreIgnoredWhenDesserializing() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final ObjectNode node = (ObjectNode) TestUtils.OBJECT_MAPPER.readTree(
                TestUtils.OBJECT_MAPPER.writeValueAsString(eventType));
        node.set("unknown_attribute", new TextNode("will just be ignored"));

        final String eventTypeName = eventType.getName();
        final String insertSQL = "INSERT INTO zn_data.event_type (et_name, et_event_type_object) " +
                "VALUES (?, to_json(?::json))";
        template.update(insertSQL,
                eventTypeName,
                TestUtils.OBJECT_MAPPER.writeValueAsString(node));

        final EventType persistedEventType = repository.findByName(eventTypeName);

        assertThat(persistedEventType, notNullValue());
    }

    private void insertEventType(final EventType eventType) throws Exception {
        final String insertSQL = "INSERT INTO zn_data.event_type (et_name, et_event_type_object) " +
                "VALUES (?, to_json(?::json))";
        template.update(insertSQL,
                eventType.getName(),
                TestUtils.OBJECT_MAPPER.writer().writeValueAsString(eventType));
    }
}
