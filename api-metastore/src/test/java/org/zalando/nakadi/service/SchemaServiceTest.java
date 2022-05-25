package org.zalando.nakadi.service;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.exception.SchemaEvolutionException;
import org.zalando.nakadi.exception.SchemaValidationException;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SchemaRepository;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.validation.JsonSchemaEnrichment;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.zalando.nakadi.domain.EventCategory.BUSINESS;

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
    private EventType eventType;
    private TimelineSync timelineSync;
    private NakadiSettings nakadiSettings;
    private NakadiAuditLogPublisher nakadiAuditLogPublisher;
    private NakadiKpiPublisher nakadiKpiPublisher;

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
        eventType = TestUtils.buildDefaultEventType();
        Mockito.when(eventTypeRepository.findByName(any())).thenReturn(eventType);
        timelineSync = Mockito.mock(TimelineSync.class);
        nakadiSettings = Mockito.mock(NakadiSettings.class);
        nakadiKpiPublisher = Mockito.mock(NakadiKpiPublisher.class);
        nakadiAuditLogPublisher = Mockito.mock(NakadiAuditLogPublisher.class);
        schemaService = new SchemaService(schemaRepository, paginationService,
                new JsonSchemaEnrichment(new DefaultResourceLoader(), "classpath:schema_metadata.json"),
                schemaEvolutionService, eventTypeRepository, adminService, authorizationValidator, eventTypeCache,
                timelineSync, nakadiSettings, nakadiAuditLogPublisher, nakadiKpiPublisher);
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
        Mockito.when(schemaRepository.getSchemaVersion(eventType.getName() + "wrong",
                eventType.getSchema().getVersion().toString()))
                .thenThrow(NoSuchSchemaException.class);
        final EventTypeSchema result = schemaService.getSchemaVersion(eventType.getName() + "wrong",
                eventType.getSchema().getVersion().toString());
    }

    @Test(expected = NoSuchSchemaException.class)
    public void testNonExistingVersionNumber() throws Exception {
        final var newVersion =
                new Version(eventType.getSchema().getVersion()).bump(Version.Level.MINOR).toString();
        Mockito.when(schemaRepository.getSchemaVersion(eventType.getName(), newVersion))
                .thenThrow(NoSuchSchemaException.class);
        schemaService.getSchemaVersion(eventType.getName(), newVersion);
    }

    @Test
    public void testGetSchemaSuccess() throws Exception {
        Mockito.when(schemaRepository.getSchemaVersion(eventType.getName(),
                eventType.getSchema().getVersion().toString()))
                .thenReturn(eventType.getSchema());
        final EventTypeSchema result =
                schemaService.getSchemaVersion(eventType.getName(), eventType.getSchema().getVersion().toString());
        Assert.assertTrue(true);
    }

    @Test
    public void invalidEventTypeSchemaJsonSchemaThenThrows() throws Exception {
        final String jsonSchemaString = Resources.toString(
                Resources.getResource("sample-invalid-json-schema.json"),
                Charsets.UTF_8);
        eventType.getSchema().setSchema(jsonSchemaString);

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType));
    }

    @Test
    public void whenPOSTBusinessEventTypeMetadataThenThrows() throws Exception {
        eventType.getSchema().setSchema(
                "{\"type\": \"object\", \"properties\": {\"metadata\": {\"type\": \"object\"} }}");
        eventType.setCategory(BUSINESS);

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType));
    }

    @Test
    public void whenEventTypeSchemaJsonIsMalformedThenThrows() throws Exception {
        eventType.getSchema().setSchema("invalid-json");

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType));
    }

    @Test
    public void whenSchemaHasIncompatibilitiesThenThrows() throws Exception {
        Mockito.doThrow(SchemaEvolutionException.class)
                .when(schemaEvolutionService).collectIncompatibilities(any());

        assertThrows(SchemaEvolutionException.class,
                () -> schemaService.validateSchema(eventType));
    }

    @Test
    public void whenPostWithRootElementOfTypeArrayThenThrows() throws Exception {
        eventType.getSchema().setSchema(
                "{\\\"type\\\":\\\"array\\\" }");
        eventType.setCategory(BUSINESS);

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType));
    }

    @Test
    public void throwsInvalidSchemaOnInvalidRegex() throws Exception {
        eventType.getSchema().setSchema("{\n" +
                "      \"properties\": {\n" +
                "        \"foo\": {\n" +
                "          \"type\": \"string\",\n" +
                "          \"pattern\": \"^(?!\\\\s*$).+\"\n" +
                "        }\n" +
                "      }\n" +
                "    }");

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType));
    }

    @Test
    public void doNotSupportSchemaWithExternalRef() {
        eventType.getSchema().setSchema("{\n" +
                "    \"properties\": {\n" +
                "      \"foo\": {\n" +
                "        \"$ref\": \"/invalid/url\"\n" +
                "      }\n" +
                "    }\n" +
                "  }");

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType));
    }

    @Test(expected = SchemaValidationException.class)
    public void testValidateSchemaEndingBracket() {
        SchemaService.isStrictlyValidJson("{\"additionalProperties\": true}}");
    }

    @Test(expected = SchemaValidationException.class)
    public void testValidateSchemaMultipleRoots() {
        SchemaService.isStrictlyValidJson("{\"additionalProperties\": true}{\"additionalProperties\": true}");
    }

    @Test(expected = SchemaValidationException.class)
    public void testValidateSchemaArbitraryEnding() {
        SchemaService.isStrictlyValidJson("{\"additionalProperties\": true}NakadiRocks");
    }

    @Test(expected = SchemaValidationException.class)
    public void testValidateSchemaArrayEnding() {
        SchemaService.isStrictlyValidJson("[{\"additionalProperties\": true}]]");
    }

    @Test(expected = SchemaValidationException.class)
    public void testValidateSchemaEndingCommaArray() {
        SchemaService.isStrictlyValidJson("[{\"test\": true},]");
    }

    @Test(expected = SchemaValidationException.class)
    public void testValidateSchemaEndingCommaArray2() {
        SchemaService.isStrictlyValidJson("[\"test\",]");
    }

    @Test(expected = SchemaValidationException.class)
    public void testValidateSchemaEndingCommaObject() {
        SchemaService.isStrictlyValidJson("{\"test\": true,}");
    }

    @Test
    public void testValidateSchemaFormattedJson() {
        SchemaService.isStrictlyValidJson("{\"properties\":{\"event_class\":{\"type\":\"string\"}," +
                "\"app_domain_id\":{\"type\":\"integer\"},\"event_type\":{\"type\":\"string\"},\"time\"" +
                ":{\"type\":\"number\"},\"partitioning_key\":{\"type\":\"string\"},\"body\":{\"type\"" +
                ":\"object\"}},\"additionalProperties\":true}");
    }
}
