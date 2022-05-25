package org.zalando.nakadi.service;

import org.apache.avro.AvroRuntimeException;
import org.everit.json.schema.Schema;
import org.everit.json.schema.SchemaException;
import org.everit.json.schema.loader.SchemaClient;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.domain.StrictJsonParser;
import org.zalando.nakadi.domain.kpi.EventTypeLogEvent;
import org.zalando.nakadi.exception.SchemaValidationException;
import org.zalando.nakadi.exceptions.runtime.EventTypeUnavailableException;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.InvalidVersionNumberException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SchemaRepository;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.util.AvroUtils;
import org.zalando.nakadi.validation.JsonSchemaEnrichment;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Component
public class SchemaService {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaService.class);

    private final SchemaRepository schemaRepository;
    private final PaginationService paginationService;
    private final JsonSchemaEnrichment jsonSchemaEnrichment;
    private final SchemaEvolutionService schemaEvolutionService;
    private final EventTypeRepository eventTypeRepository;
    private final AdminService adminService;
    private final AuthorizationValidator authorizationValidator;
    private final EventTypeCache eventTypeCache;
    private final TimelineSync timelineSync;
    private final NakadiSettings nakadiSettings;
    private final NakadiAuditLogPublisher nakadiAuditLogPublisher;
    private final NakadiKpiPublisher nakadiKpiPublisher;

    @Autowired
    public SchemaService(final SchemaRepository schemaRepository,
                         final PaginationService paginationService,
                         final JsonSchemaEnrichment jsonSchemaEnrichment,
                         final SchemaEvolutionService schemaEvolutionService,
                         final EventTypeRepository eventTypeRepository,
                         final AdminService adminService,
                         final AuthorizationValidator authorizationValidator,
                         final EventTypeCache eventTypeCache,
                         final TimelineSync timelineSync,
                         final NakadiSettings nakadiSettings,
                         final NakadiAuditLogPublisher nakadiAuditLogPublisher,
                         final NakadiKpiPublisher nakadiKpiPublisher) {
        this.schemaRepository = schemaRepository;
        this.paginationService = paginationService;
        this.jsonSchemaEnrichment = jsonSchemaEnrichment;
        this.schemaEvolutionService = schemaEvolutionService;
        this.eventTypeRepository = eventTypeRepository;
        this.adminService = adminService;
        this.authorizationValidator = authorizationValidator;
        this.eventTypeCache = eventTypeCache;
        this.timelineSync = timelineSync;
        this.nakadiSettings = nakadiSettings;
        this.nakadiAuditLogPublisher = nakadiAuditLogPublisher;
        this.nakadiKpiPublisher = nakadiKpiPublisher;
    }

    public void addSchema(final String eventTypeName, final EventTypeSchemaBase newSchema) {
        Closeable closeable = null;
        try {
            closeable = timelineSync.workWithEventType(eventTypeName, nakadiSettings.getTimelineWaitTimeoutMs());
            final EventType originalEventType = eventTypeRepository.findByName(eventTypeName);

            if (!adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
                authorizationValidator.authorizeEventTypeAdmin(originalEventType);
            }

            final EventTypeBase updatedEventType = new EventTypeBase(originalEventType);
            updatedEventType.setSchema(newSchema);

            final EventType eventType = getValidEvolvedEventType(originalEventType, updatedEventType);
            eventTypeRepository.update(eventType);

            eventTypeCache.invalidate(eventType.getName());
            if (!eventType.getSchema().getVersion().equals(originalEventType.getSchema().getVersion())) {
                nakadiKpiPublisher.publish(() -> new EventTypeLogEvent()
                        .setEventType(eventTypeName)
                        .setStatus("updated")
                        .setCategory(eventType.getCategory().name())
                        .setAuthz(eventType.getAuthorization() == null ? "disabled" : "enabled")
                        .setCompatibilityMode(eventType.getCompatibilityMode().name()));

                nakadiAuditLogPublisher.publish(Optional.of(originalEventType), Optional.of(eventType),
                        NakadiAuditLogPublisher.ResourceType.EVENT_TYPE, NakadiAuditLogPublisher.ActionType.UPDATED,
                        eventType.getName());
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventTypeUnavailableException("Event type " + eventTypeName
                    + " is currently in maintenance, please repeat request");
        } catch (final TimeoutException e) {
            throw new EventTypeUnavailableException("Event type " + eventTypeName
                    + " is currently in maintenance, please repeat request");
        } finally {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            } catch (final IOException e) {
                LOG.error("Exception occurred when releasing usage of event-type", e);
            }
        }
    }

    public EventType getValidEvolvedEventType(final EventType originalEventType, final EventTypeBase updatedEventType) {
        validateSchema(updatedEventType);
        return schemaEvolutionService.evolve(originalEventType, updatedEventType);
    }

    public PaginationWrapper getSchemas(final String name, final int offset, final int limit)
            throws InvalidLimitException {
        if (limit < 1 || limit > 1000) {
            throw new InvalidLimitException("'limit' parameter sholud have value between 1 and 1000");
        }

        if (offset < 0) {
            throw new InvalidLimitException("'offset' parameter can't be lower than 0");
        }

        return paginationService
                .paginate(offset, limit, String.format("/event-types/%s/schemas", name),
                        (o, l) -> schemaRepository.getSchemas(name, o, l),
                        () -> schemaRepository.getSchemasCount(name));
    }

    public EventTypeSchema getSchemaVersion(final String name, final String version)
            throws NoSuchSchemaException, InvalidVersionNumberException {
        final EventTypeSchema schema = schemaRepository.getSchemaVersion(name, version);
        return schema;
    }

    public Optional<EventTypeSchema> getLatestSchemaForType(final String name, final EventTypeSchema.Type schemaType) {
        return schemaRepository.getLatestSchemaForType(name, schemaType);
    }

    public void validateSchema(final EventTypeBase eventType) throws SchemaValidationException {
        try {
            final String eventTypeSchema = eventType.getSchema().getSchema();


            final EventTypeSchemaBase.Type schemaType = eventType.getSchema().getType();
            if (schemaType.equals(EventTypeSchemaBase.Type.JSON_SCHEMA)) {
                isStrictlyValidJson(eventTypeSchema);
                validateJsonTypeSchema(eventType, eventTypeSchema);
            } else if (schemaType.equals(EventTypeSchemaBase.Type.AVRO_SCHEMA)) {
                validateAvroTypeSchema(eventTypeSchema);
            } else {
                throw new IllegalArgumentException("undefined schema type");
            }

        } catch (final com.google.re2j.PatternSyntaxException e) {
            throw new SchemaValidationException("invalid regex pattern in the schema: "
                    + e.getDescription() + " \"" + e.getPattern() + "\"");
        } catch (final JSONException e) {
            throw new SchemaValidationException("schema must be a valid json");
        } catch (final SchemaException e) {
            throw new SchemaValidationException("schema must be a valid json-schema");
        }
    }

    private void validateAvroTypeSchema(final String eventTypeSchema) {
        try {
            AvroUtils.getParsedSchema(eventTypeSchema);
        } catch (AvroRuntimeException e) {
            throw new SchemaValidationException("failed to parse avro schema " + e.getMessage());
        }
    }

    private void validateJsonTypeSchema(final EventTypeBase eventType, final String eventTypeSchema) {
        final JSONObject schemaAsJson = new JSONObject(eventTypeSchema);

        if (schemaAsJson.has("type") && !Objects.equals("object", schemaAsJson.getString("type"))) {
            throw new SchemaValidationException("\"type\" of root element in schema can only be \"object\"");
        }

        final Schema schema = SchemaLoader
                .builder()
                .httpClient(new BlockedHttpClient())
                .schemaJson(schemaAsJson)
                .build()
                .load()
                .build();

        if (eventType.getCategory() == EventCategory.BUSINESS && schema.definesProperty("#/metadata")) {
            throw new SchemaValidationException("\"metadata\" property is reserved");
        }

        final List<String> orderingInstanceIds = eventType.getOrderingInstanceIds();
        final List<String> orderingKeyFields = eventType.getOrderingKeyFields();
        if (!orderingInstanceIds.isEmpty() && orderingKeyFields.isEmpty()) {
            throw new SchemaValidationException(
                    "`ordering_instance_ids` field can not be defined without defining `ordering_key_fields`");
        }
        final JSONObject effectiveSchemaAsJson = jsonSchemaEnrichment.effectiveSchema(eventType, schemaAsJson);
        final Schema effectiveSchema = SchemaLoader.load(effectiveSchemaAsJson);
        validateFieldsInSchema("ordering_key_fields", orderingKeyFields, effectiveSchema);
        validateFieldsInSchema("ordering_instance_ids", orderingInstanceIds, effectiveSchema);

        if (eventType.getCompatibilityMode() == CompatibilityMode.COMPATIBLE) {
            validateJsonSchemaConstraints(schemaAsJson);
        }
    }

    private void validateJsonSchemaConstraints(final JSONObject schema) throws SchemaValidationException {
        final List<SchemaIncompatibility> incompatibilities = schemaEvolutionService.collectIncompatibilities(schema);

        if (!incompatibilities.isEmpty()) {
            final String errorMessage = incompatibilities.stream().map(Object::toString)
                    .collect(Collectors.joining(", "));
            throw new SchemaValidationException("Invalid schema: " + errorMessage);
        }
    }

    private void validateFieldsInSchema(final String fieldName, final List<String> fields, final Schema schema) {
        final List<String> absentFields = fields.stream()
                .filter(field -> !schema.definesProperty(convertToJSONPointer(field)))
                .collect(Collectors.toList());
        if (!absentFields.isEmpty()) {
            throw new SchemaValidationException(fieldName + " " + absentFields + " absent in schema");
        }
    }

    private String convertToJSONPointer(final String value) {
        return value.replaceAll("\\.", "/");
    }

    private class BlockedHttpClient implements SchemaClient {
        @Override
        public InputStream get(final String ref) throws SchemaValidationException {
            throw new SchemaValidationException("external url reference is not supported: " + ref);
        }
    }

    public static void isStrictlyValidJson(final String jsonInString) throws InvalidEventTypeException {
        try {
            StrictJsonParser.parse(jsonInString, false);
        } catch (final RuntimeException jpe) {
            throw new SchemaValidationException("schema must be a valid json: " + jpe.getMessage());
        }
    }
}
