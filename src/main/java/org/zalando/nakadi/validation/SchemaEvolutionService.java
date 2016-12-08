package org.zalando.nakadi.validation;


import com.google.common.collect.Lists;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.SchemaChange;
import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.validation.schema.ForbiddenAttributeIncompatibility;
import org.zalando.nakadi.validation.schema.diff.SchemaDiff;
import org.zalando.nakadi.validation.schema.SchemaEvolutionConstraint;
import org.zalando.nakadi.validation.schema.SchemaEvolutionIncompatibility;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_ITEMS_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.ADDITIONAL_PROPERTIES_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.DESCRIPTION_CHANGED;
import static org.zalando.nakadi.domain.SchemaChange.Type.PROPERTIES_ADDED;
import static org.zalando.nakadi.domain.SchemaChange.Type.TITLE_CHANGED;
import static org.zalando.nakadi.domain.Version.Level.MAJOR;
import static org.zalando.nakadi.domain.Version.Level.NO_CHANGES;

public class SchemaEvolutionService {

    private final List<SchemaEvolutionConstraint> schemaEvolutionConstraints;
    private final Schema metaSchema;
    private final SchemaDiff schemaDiff;
    private final Map<SchemaChange.Type, Version.Level> changeToLevel;
    private final Map<SchemaChange.Type, String> errorMessages;
    private static final List<SchemaChange.Type> FIXED_TO_COMPATIBLE_ALLOWED_CHANGES = Lists.newArrayList(
            DESCRIPTION_CHANGED, TITLE_CHANGED, PROPERTIES_ADDED, ADDITIONAL_PROPERTIES_CHANGED,
            ADDITIONAL_ITEMS_CHANGED);

    public SchemaEvolutionService(final Schema metaSchema,
                                  final List<SchemaEvolutionConstraint> schemaEvolutionConstraints,
                                  final SchemaDiff schemaDiff,
                                  final Map<SchemaChange.Type, Version.Level> changeToLevel,
                                  final Map<SchemaChange.Type, String> errorMessages) {
        this.metaSchema = metaSchema;
        this.schemaEvolutionConstraints = schemaEvolutionConstraints;
        this.schemaDiff = schemaDiff;
        this.changeToLevel = changeToLevel;
        this.errorMessages = errorMessages;
    }

    public List<SchemaIncompatibility> collectIncompatibilities(final JSONObject schemaJson) {
        final List<SchemaIncompatibility> incompatibilities = new ArrayList<>();

        try {
            metaSchema.validate(schemaJson);
        } catch (final ValidationException e) {
            collectErrorMessages(e, incompatibilities);
        }

        return incompatibilities;
    }

    private void collectErrorMessages(final ValidationException e,
                                      final List<SchemaIncompatibility> incompatibilities) {
        if (e.getCausingExceptions().isEmpty()) {
            incompatibilities.add(
                    new ForbiddenAttributeIncompatibility(e.getPointerToViolation(), e.getErrorMessage()));
        } else {
            e.getCausingExceptions()
                    .forEach(causingException -> collectErrorMessages(causingException, incompatibilities));
        }
    }

    public EventType evolve(final EventType original, final EventTypeBase eventType) throws InvalidEventTypeException {
        final Optional<SchemaEvolutionIncompatibility> incompatibility = schemaEvolutionConstraints.stream()
                .map(c -> c.validate(original, eventType)).filter(Optional::isPresent).findFirst()
                .orElse(Optional.empty());

        if (incompatibility.isPresent()) {
            throw new InvalidEventTypeException(incompatibility.get().getReason());
        }

        final List<SchemaChange> changes = schemaDiff.collectChanges(schema(original), schema(eventType));

        final Version.Level changeLevel = semanticOfChange(changes);

        validateCompatibleModeChanges(original, changes, changeLevel);
        validateCompatibilityModeMigration(original, eventType, changes);

        return this.bumpVersion(original, eventType, changeLevel);
    }

    private void validateCompatibilityModeMigration(final EventType original, final EventTypeBase eventType,
                                                    final List<SchemaChange> changes) throws InvalidEventTypeException {
        if (original.getCompatibilityMode() == CompatibilityMode.FIXED
            && eventType.getCompatibilityMode() == CompatibilityMode.COMPATIBLE) {

            final List<SchemaChange> forbiddenChanges = changes.stream()
                    .filter(change -> !FIXED_TO_COMPATIBLE_ALLOWED_CHANGES.contains(change.getType()))
                    .collect(Collectors.toList());
            if (!forbiddenChanges.isEmpty()) {
                final String errorMessage = forbiddenChanges.stream().map(this::formatErrorMessage)
                        .collect(Collectors.joining(", "));
                throw new InvalidEventTypeException("Invalid schema: " + errorMessage);
            }
        }
    }

    private void validateCompatibleModeChanges(final EventType original, final List<SchemaChange> changes,
                                               final Version.Level changeLevel) throws InvalidEventTypeException {
        if (original.getCompatibilityMode() == CompatibilityMode.COMPATIBLE && changeLevel == MAJOR) {
            final String errorMessage = changes.stream()
                    .filter(change -> MAJOR.equals(changeToLevel.get(change.getType())))
                    .map(this::formatErrorMessage)
                    .collect(Collectors.joining(", "));
            throw new InvalidEventTypeException("Invalid schema: " + errorMessage);
        }
    }

    private String formatErrorMessage(final SchemaChange change) {
        return change.getJsonPath() + ": " + errorMessages.get(change.getType());
    }

    private Version.Level semanticOfChange(final List<SchemaChange> changes) {
        return changes.stream().map(SchemaChange::getType).map(changeToLevel::get).reduce(NO_CHANGES, (acc, change) -> {
            if (Version.Level.valueOf(change.toString()).ordinal() < Version.Level.valueOf(acc.toString()).ordinal()) {
                return change;
            } else {
                return acc;
            }
        });
    }

    private Schema schema(final EventTypeBase eventType) {
        final JSONObject schemaAsJson = new JSONObject(eventType.getSchema().getSchema());

        return SchemaLoader.load(schemaAsJson);
    }

    private EventType bumpVersion(final EventType original, final EventTypeBase eventType,
                                  final Version.Level changeLevel) {
        final DateTime now = new DateTime(DateTimeZone.UTC);
        final String newVersion = original.getSchema().getVersion().bump(changeLevel).toString();

        return new EventType(eventType, newVersion, original.getCreatedAt(), now);
    }
}
