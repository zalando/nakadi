package org.zalando.nakadi.service;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SchemaRepository;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.validation.JsonSchemaEnrichment;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.any;
import static org.zalando.nakadi.domain.EventCategory.BUSINESS;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public class SchemaServiceTest {

    private SchemaRepository schemaRepository;
    private PaginationService paginationService;
    private SchemaService schemaService;
    private JsonSchemaEnrichment jsonSchemaEnrichment;
    private SchemaEvolutionService schemaEvolutionService;
    private EventTypeRepository eventTypeRepository;
    private AdminService adminService;
    private AuthorizationValidator authorizationValidator;
    private EventTypeCache eventTypeCache;

    @Before
    public void setUp() throws IOException {
        schemaRepository = Mockito.mock(SchemaRepository.class);
        paginationService = Mockito.mock(PaginationService.class);
        jsonSchemaEnrichment = Mockito.mock(JsonSchemaEnrichment.class);
        schemaEvolutionService = Mockito.mock(SchemaEvolutionService.class);
        eventTypeRepository = Mockito.mock(EventTypeRepository.class);
        adminService = Mockito.mock(AdminService.class);
        authorizationValidator = Mockito.mock(AuthorizationValidator.class);
        eventTypeCache = Mockito.mock(EventTypeCache.class);
        schemaService = new SchemaService(schemaRepository, paginationService,
                new JsonSchemaEnrichment(new DefaultResourceLoader(), "classpath:schema_metadata.json"),
                schemaEvolutionService, eventTypeRepository, adminService, authorizationValidator, eventTypeCache);
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

    @Test
    public void invalidEventTypeSchemaJsonSchemaThenThrows() throws Exception {
        final EventType eventType = TestUtils.buildDefaultEventType();

        final String jsonSchemaString = Resources.toString(
                Resources.getResource("sample-invalid-json-schema.json"),
                Charsets.UTF_8);
        eventType.getSchema().setSchema(jsonSchemaString);

        assertThrows(InvalidEventTypeException.class, () -> schemaService.validateSchema(eventType));
    }

    @Test
    public void whenPOSTBusinessEventTypeMetadataThenThrows() throws Exception {
        final EventType eventType = TestUtils.buildDefaultEventType();
        eventType.getSchema().setSchema(
                "{\"type\": \"object\", \"properties\": {\"metadata\": {\"type\": \"object\"} }}");
        eventType.setCategory(BUSINESS);

        assertThrows(InvalidEventTypeException.class, () -> schemaService.validateSchema(eventType));
    }

    @Test
    public void whenEventTypeSchemaJsonIsMalformedThenThrows() throws Exception {
        final EventType eventType = TestUtils.buildDefaultEventType();
        eventType.getSchema().setSchema("invalid-json");

        assertThrows(InvalidEventTypeException.class, () -> schemaService.validateSchema(eventType));
    }

    @Test
    public void whenSchemaHasIncompatibilitiesThenThrows() throws Exception {
        final EventType eventType = TestUtils.buildDefaultEventType();
        
        Mockito.doThrow(InvalidEventTypeException.class).when(schemaEvolutionService).collectIncompatibilities(any());

        assertThrows(InvalidEventTypeException.class, () -> schemaService.validateSchema(eventType));
    }

    @Test
    public void whenPostWithRootElementOfTypeArrayThenThrows() throws Exception {
        final EventType eventType = TestUtils.buildDefaultEventType();
        eventType.getSchema().setSchema(
                "{\\\"type\\\":\\\"array\\\" }");
        eventType.setCategory(BUSINESS);

        assertThrows(InvalidEventTypeException.class, () -> schemaService.validateSchema(eventType));
    }

    @Test
    public void throwsInvalidSchemaOnInvalidRegex() throws Exception {
        final EventType et = TestUtils.buildDefaultEventType();
        et.getSchema().setSchema("{\n" +
                "      \"properties\": {\n" +
                "        \"foo\": {\n" +
                "          \"type\": \"string\",\n" +
                "          \"pattern\": \"^(?!\\\\s*$).+\"\n" +
                "        }\n" +
                "      }\n" +
                "    }");

        assertThrows(InvalidEventTypeException.class, () -> schemaService.validateSchema(et));
    }

    @Test
    public void doNotSupportSchemaWithExternalRef() {
        final EventType eventType = TestUtils.buildDefaultEventType();
        eventType.getSchema().setSchema("{\n" +
                "    \"properties\": {\n" +
                "      \"foo\": {\n" +
                "        \"$ref\": \"/invalid/url\"\n" +
                "      }\n" +
                "    }\n" +
                "  }");

        assertThrows(InvalidEventTypeException.class, () -> schemaService.validateSchema(eventType));
    }
}