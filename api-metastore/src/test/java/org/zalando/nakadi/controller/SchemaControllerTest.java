package org.zalando.nakadi.controller;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.MapBindingResult;
import org.springframework.validation.beanvalidation.SpringValidatorAdapter;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.exceptions.runtime.SchemaEvolutionException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ValidationException;
import org.zalando.nakadi.model.CompatibilityResponse;
import org.zalando.nakadi.model.SchemaWrapper;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import javax.validation.Validation;
import java.util.HashMap;

import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public class SchemaControllerTest {

    private SpringValidatorAdapter validator;

    private SchemaService schemaService;
    private NativeWebRequest nativeWebRequest;
    private EventTypeService eventTypeService;
    private AdminService adminService;
    private AuthorizationValidator authorizationValidator;
    private NakadiAuditLogPublisher nakadiAuditLogPublisher;
    private NakadiKpiPublisher nakadiKpiPublisher;
    private SchemaController schemaController;

    @Before
    public void setUp() {
        validator = new SpringValidatorAdapter(Validation.buildDefaultValidatorFactory().getValidator());

        schemaService = Mockito.mock(SchemaService.class);
        nativeWebRequest = Mockito.mock(NativeWebRequest.class);
        eventTypeService = Mockito.mock(EventTypeService.class);
        adminService = Mockito.mock(AdminService.class);
        authorizationValidator = Mockito.mock(AuthorizationValidator.class);
        nakadiAuditLogPublisher = Mockito.mock(NakadiAuditLogPublisher.class);
        nakadiKpiPublisher = Mockito.mock(NakadiKpiPublisher.class);

        schemaController = new SchemaController(schemaService, eventTypeService, adminService, authorizationValidator,
                nakadiAuditLogPublisher, nakadiKpiPublisher);
    }

    @Test
    public void testSuccess() {
        Mockito.when(schemaService.getSchemas("et_test", 0, 1)).thenReturn(null);
        Mockito.when(eventTypeService.get("et_test")).thenReturn(EventTypeTestBuilder.builder().build());

        final ResponseEntity<?> result =
            schemaController.getSchemas("et_test", 0, 1, nativeWebRequest);

        Assert.assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    public void testGetLatestSchemaVersionThen200() {
        final EventType eventType = buildDefaultEventType();
        Mockito.when(eventTypeService.get(eventType.getName())).thenReturn(eventType);

        final ResponseEntity<?> result = schemaController
                .getSchemaVersion(eventType.getName(), "latest", nativeWebRequest);

        Assert.assertEquals(HttpStatus.OK, result.getStatusCode());
        Assert.assertEquals(eventType.getSchema().toString(), result.getBody().toString());
    }

    @Test(expected = NoSuchEventTypeException.class)
    public void testGetLatestSchemaVersionWrongEventTypeThen404() {

        Mockito.when(eventTypeService.get("et_wrong_event"))
                .thenThrow(new NoSuchEventTypeException("no event type"));

        schemaController.getSchemaVersion("et_wrong_event", "latest", nativeWebRequest);
    }

    @Test
    public void testGetLatestSchemaVersionByNumberThen200() {
        final EventType eventType = buildDefaultEventType();
        Mockito.when(schemaService.getSchemaVersion(eventType.getName(),
                eventType.getSchema().getVersion().toString())).thenReturn(eventType.getSchema());

        final ResponseEntity<?> result =
                schemaController.getSchemaVersion(eventType.getName(),
                        eventType.getSchema().getVersion().toString(), nativeWebRequest);

        Assert.assertEquals(HttpStatus.OK, result.getStatusCode());
        Assert.assertEquals(eventType.getSchema().toString(), result.getBody().toString());
    }

    @Test
    public void testCheckCompatibilityForValidEvolution() {
        final EventType eventTypeOriginal = buildDefaultEventType();
        final EventType eventTypeNew = buildDefaultEventType();

        final var schemeBase = new EventTypeSchemaBase(EventTypeSchemaBase.Type.JSON_SCHEMA,
                eventTypeOriginal.getSchema().getSchema());
        eventTypeNew.setSchema(new EventTypeSchema(schemeBase, "1.1.0", DateTime.now()));

        Mockito.when(eventTypeService.get(eventTypeOriginal.getName())).
                thenReturn(eventTypeOriginal);
        Mockito.when(schemaService.
                        getValidEvolvedEventType(eventTypeOriginal, eventTypeNew)).
                thenReturn(eventTypeNew);

        final ResponseEntity<?> result = schemaController
                .checkCompatibility(
                        eventTypeOriginal.getName(),
                        "latest",
                        new SchemaWrapper(eventTypeNew.getSchema().getSchema()),
                        new MapBindingResult(new HashMap<>(), "name"));

        Assert.assertEquals(HttpStatus.OK, result.getStatusCode());
        final var resp = (CompatibilityResponse) result.getBody();
        final var expected = new CompatibilityResponse(true, CompatibilityMode.COMPATIBLE, null, "1.0.0");
        Assert.assertEquals(expected, resp);

    }

    @Test
    public void testCheckCompatibilityForIncompatibleEvolution() {
        final var origSchema = "[{\"type\":\"record\",\"name\":\"NAME_PLACE_HOLDER\"," +
                "\"fields\":[{\"name\":\"foo\",\"type\":\"string\"},{\"name\":\"bar\",\"type\":\"string\"}," +
                "{\"name\":\"baz\",\"type\":\"int\"}]";
        final var etSchema = new EventTypeSchema(
                new EventTypeSchemaBase(EventTypeSchemaBase.Type.AVRO_SCHEMA, origSchema), "1", DateTime.now()
        );
        final EventType eventTypeOriginal =
                EventTypeTestBuilder.builder().
                        compatibilityMode(CompatibilityMode.FORWARD).
                        schema(etSchema).build();

        final var newSchema = "[{\"type\":\"record\",\"name\":\"NAME_PLACE_HOLDER\"," +
                "\"fields\":[{\"name\":\"foo\",\"type\":\"string\"},{\"name\":\"bar\",\"type\":\"string\"}]";
        final var newEtSchema = new EventTypeSchema(
                new EventTypeSchemaBase(EventTypeSchemaBase.Type.AVRO_SCHEMA, newSchema), "2", DateTime.now()
        );

        final EventType eventTypeNew =
                EventTypeTestBuilder.builder().
                        schema(newEtSchema).build();

        Mockito.when(eventTypeService.get(eventTypeOriginal.getName())).
                thenReturn(eventTypeOriginal);
        final var errorMsg = "Failed to evolve avro schema due to " +
                "[{ type:READER_FIELD_MISSING_DEFAULT_VALUE, location:/fields/2, message:baz }]";
        Mockito.when(schemaService.
                        getValidEvolvedEventType(Mockito.eq(eventTypeOriginal), Mockito.any())).
                thenThrow(new SchemaEvolutionException(errorMsg));

        final ResponseEntity<?> result = schemaController
                .checkCompatibility(
                        eventTypeOriginal.getName(),
                        "latest",
                        new SchemaWrapper(eventTypeNew.getSchema().getSchema()),
                        new MapBindingResult(new HashMap<>(), "name"));

        Assert.assertEquals(HttpStatus.OK, result.getStatusCode());
        final var resp = (CompatibilityResponse) result.getBody();
        final var expected = new CompatibilityResponse(false, CompatibilityMode.FORWARD, errorMsg, "1");
        Assert.assertEquals(expected, resp);
    }

    @Test(expected = ValidationException.class)
    public void testValidationExceptionOnSchemaNullWhenCompatibilityChecked() {
        final var csr = new SchemaWrapper(null);
        final var errors = new BeanPropertyBindingResult(csr, "csr");
        validator.validate(csr, errors);

        schemaController.checkCompatibility("name", "latest", csr, errors);
    }

    @Test(expected = ValidationException.class)
    public void testValidationExceptionOnNullSchemaWhenSchemaCreated() {
        final var etSchemaBase = new EventTypeSchemaBase(EventTypeSchemaBase.Type.AVRO_SCHEMA, null);
        final var errors = new BeanPropertyBindingResult(etSchemaBase, "etSchemaBase");
        validator.validate(etSchemaBase, errors);

        schemaController.create("test", etSchemaBase, false, errors);
    }

    @Test(expected = ValidationException.class)
    public void testValidationExceptionOnNullTypeWhenSchemaCreated() {
        final var etSchemaBase = new EventTypeSchemaBase(null, "");
        final var errors = new BeanPropertyBindingResult(etSchemaBase, "etSchemaBase");
        validator.validate(etSchemaBase, errors);

        schemaController.create("test", etSchemaBase, false, errors);
    }
}
