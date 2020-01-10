package org.zalando.nakadi.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.repository.db.SchemaRepository;

import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public class SchemaServiceTest {

    private SchemaRepository schemaRepository;
    private PaginationService paginationService;
    private SchemaService schemaService;

    @Before
    public void setUp() {
        schemaRepository = Mockito.mock(SchemaRepository.class);
        paginationService = Mockito.mock(PaginationService.class);
        schemaService = new SchemaService(schemaRepository, paginationService);
    }

    @Test(expected = InvalidLimitException.class)
    public void testOffsetBounds() {
        schemaService.getSchemas("name", -1, 1);
    }

    @Test(expected = InvalidLimitException.class)
    public void testLimitLowerBounds() {
        schemaService.getSchemas("name", 0, 0);
    }

    @Test(expected = InvalidLimitException.class)
    public void testLimitUpperBounds() {
        schemaService.getSchemas("name", 0, 1001);
    }

    @Test
    public void testSuccess() {
        final PaginationWrapper result = schemaService.getSchemas("name", 0, 1000);
        Assert.assertTrue(true);
    }

    @Test(expected = NoSuchSchemaException.class)
    public void testIllegalVersionNumber() throws Exception {
        final EventType eventType = buildDefaultEventType();
        Mockito.when(schemaRepository.getSchemaVersion(eventType.getName() + "wrong",
                eventType.getSchema().getVersion().toString()))
                .thenThrow(NoSuchSchemaException.class);
        final EventTypeSchema result = schemaService.getSchemaVersion(eventType.getName() + "wrong",
                eventType.getSchema().getVersion().toString());
    }

    @Test(expected = NoSuchSchemaException.class)
    public void testNonExistingVersionNumber() throws Exception {
        final EventType eventType = buildDefaultEventType();
        Mockito.when(schemaRepository.getSchemaVersion(eventType.getName(),
                eventType.getSchema().getVersion().bump(Version.Level.MINOR).toString()))
                .thenThrow(NoSuchSchemaException.class);
        schemaService.getSchemaVersion(eventType.getName(),
                eventType.getSchema().getVersion().bump(Version.Level.MINOR).toString());
    }

    @Test
    public void testGetSchemaSuccess() throws Exception {
        final EventType eventType = buildDefaultEventType();
        Mockito.when(schemaRepository.getSchemaVersion(eventType.getName(),
                eventType.getSchema().getVersion().toString()))
                .thenReturn(eventType.getSchema());
        final EventTypeSchema result =
                schemaService.getSchemaVersion(eventType.getName(), eventType.getSchema().getVersion().toString());
        Assert.assertTrue(true);
    }

}