package org.zalando.nakadi.controller;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;

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
        Mockito.when(schemaService.getSchemas("et_test", 0, 1)).thenReturn(Result.ok(null));
        Mockito.when(eventTypeService.get("et_test")).thenReturn(Result.ok(EventTypeTestBuilder.builder().build()));
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService)
                        .getSchemas("et_test", 0, 1, nativeWebRequest);
        Assert.assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    public void testFailure503() {
        Mockito.when(eventTypeService.get("et_test")).thenReturn(Result.ok(EventTypeTestBuilder.builder().build()));
        Mockito.when(schemaService.getSchemas("et_test", 0, 1))
                .thenReturn(Result.problem(Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE)));
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService)
                        .getSchemas("et_test", 0, 1, nativeWebRequest);
        Assert.assertEquals(HttpStatus.SERVICE_UNAVAILABLE,  result.getStatusCode());
    }

    @Test
    public void testGetLatestSchemaVersionThen200() {
        final EventType eventType = buildDefaultEventType();
        Mockito.when(eventTypeService.get(eventType.getName())).thenReturn(Result.ok(eventType));
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService)
                        .getSchemaVersion(eventType.getName(), "latest", nativeWebRequest);
        Assert.assertEquals(HttpStatus.OK, result.getStatusCode());
        Assert.assertEquals(eventType.getSchema().toString(), result.getBody().toString());
    }

    @Test
    public void testGetLatestSchemaVersionWrongEventTypeThen404() {

        Mockito.when(eventTypeService.get("et_wrong_event"))
                .thenReturn(Result.problem(Problem.valueOf(Response.Status.NOT_FOUND)));
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService)
                        .getSchemaVersion("et_wrong_event", "latest", nativeWebRequest);
        Assert.assertEquals(HttpStatus.NOT_FOUND, result.getStatusCode());
    }

    @Test
    public void testGetLatestSchemaVersionByNumberThen200() {
        final EventType eventType = buildDefaultEventType();
        Mockito.when(schemaService.getSchemaVersion(eventType.getName(),
                eventType.getSchema().getVersion().toString())).thenReturn(Result.ok(eventType.getSchema()));
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService).getSchemaVersion(eventType.getName(),
                        eventType.getSchema().getVersion().toString(), nativeWebRequest);
        Assert.assertEquals(HttpStatus.OK, result.getStatusCode());
        Assert.assertEquals(eventType.getSchema().toString(), result.getBody().toString());
    }

    @Test
    public void testGetIllegalSchemaVersionThen404() {
        Mockito.when(schemaService.getSchemaVersion("et_test_event", "illegal"))
                .thenReturn(Result.problem(Problem.valueOf(Response.Status.NOT_FOUND)));
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService)
                        .getSchemaVersion("et_test_event", "illegal", nativeWebRequest);
        Assert.assertEquals(HttpStatus.NOT_FOUND, result.getStatusCode());
    }
    public void testFailure404() {
        Mockito.when(eventTypeService.get("et_test"))
                .thenReturn(Result.problem(Problem.valueOf(Response.Status.NOT_FOUND)));
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService)
                        .getSchemas("et_test", 0, 1, nativeWebRequest);
        Assert.assertEquals(HttpStatus.NOT_FOUND,  result.getStatusCode());
    }
}
