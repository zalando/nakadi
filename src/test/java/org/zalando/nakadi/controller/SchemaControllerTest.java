package org.zalando.nakadi.controller;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public class SchemaControllerTest {

    private SchemaService schemaService;
    private NativeWebRequest nativeWebRequest;
    private EventTypeService eventTypeService;

    @Before
    public void setUp() {
        schemaService = Mockito.mock(SchemaService.class);
        nativeWebRequest = Mockito.mock(NativeWebRequest.class);
        eventTypeService = Mockito.mock(EventTypeService.class);
    }

    @Test
    public void testSuccess() {
        Mockito.when(schemaService.getSchemas("et_test", 0, 1)).thenReturn(null);
        Mockito.when(eventTypeService.get("et_test")).thenReturn(EventTypeTestBuilder.builder().build());
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService)
                        .getSchemas("et_test", 0, 1, nativeWebRequest);
        Assert.assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    public void testGetLatestSchemaVersionThen200() {
        final EventType eventType = buildDefaultEventType();
        Mockito.when(eventTypeService.get(eventType.getName())).thenReturn(eventType);
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService)
                        .getSchemaVersion(eventType.getName(), "latest", nativeWebRequest);
        Assert.assertEquals(HttpStatus.OK, result.getStatusCode());
        Assert.assertEquals(eventType.getSchema().toString(), result.getBody().toString());
    }

    @Test(expected = NoSuchEventTypeException.class)
    public void testGetLatestSchemaVersionWrongEventTypeThen404() {

        Mockito.when(eventTypeService.get("et_wrong_event"))
                .thenThrow(new NoSuchEventTypeException("no event type"));
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService)
                        .getSchemaVersion("et_wrong_event", "latest", nativeWebRequest);
    }

    @Test
    public void testGetLatestSchemaVersionByNumberThen200() {
        final EventType eventType = buildDefaultEventType();
        Mockito.when(schemaService.getSchemaVersion(eventType.getName(),
                eventType.getSchema().getVersion().toString())).thenReturn(eventType.getSchema());
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService).getSchemaVersion(eventType.getName(),
                        eventType.getSchema().getVersion().toString(), nativeWebRequest);
        Assert.assertEquals(HttpStatus.OK, result.getStatusCode());
        Assert.assertEquals(eventType.getSchema().toString(), result.getBody().toString());
    }
}
