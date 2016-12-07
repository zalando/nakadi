package org.zalando.nakadi.controller;

import org.springframework.http.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;

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
        Mockito.when(schemaService.getSchemas("et_test_name", 0, 1)).thenReturn(Result.ok(null));
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService)
                        .getSchemas("et_test_name", 0, 1, nativeWebRequest);
        Assert.assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    public void testFailure() {
        Mockito.when(schemaService.getSchemas("et_test_name", 0, 1))
                .thenReturn(Result.problem(Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE)));
        final ResponseEntity<?> result =
                new SchemaController(schemaService, eventTypeService)
                        .getSchemas("et_test_name", 0, 1, nativeWebRequest);
        Assert.assertEquals(HttpStatus.SERVICE_UNAVAILABLE,  result.getStatusCode());
    }

}