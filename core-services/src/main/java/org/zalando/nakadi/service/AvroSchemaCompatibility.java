package org.zalando.nakadi.service;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.Incompatibility;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.CompatibilityMode;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class AvroSchemaCompatibility {

    private static BiFunction<Schema, Schema, SchemaPairCompatibility> backwardCompatibleFn = (reader, writer) ->
            SchemaCompatibility.checkReaderWriterCompatibility(writer, reader);

    private static BiFunction<Schema, Schema, SchemaPairCompatibility> forwardCompatibleFn = (reader, writer) ->
            SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);

    private static BiFunction<Schema, Schema, List<SchemaPairCompatibility>> fullyCompatibleFn = (reader, writer) ->
            List.of(backwardCompatibleFn.apply(reader, writer), forwardCompatibleFn.apply(reader, writer));


    /**
     * Evaluate schema evolution for a given list of Schemas and a new Schema provided a CompatibilityMode.
     *
     * @param original
     * @param newSchema
     * @param compatibilityMode
     * @return List of Incompatibilities
     */
    public List<AvroIncompatibility> validateSchema(final List<Schema> original, final Schema newSchema,
                                                    final CompatibilityMode compatibilityMode)
                                                    throws AvroRuntimeException {
        if (original == null || original.isEmpty()){
            throw new IllegalArgumentException("previous schemas cannot be empty");
        }
        if (newSchema == null){
            throw new IllegalArgumentException("new schema cannot be null");
        }

        final Supplier<Schema> getLastSchema = () -> original.get(original.size() - 1);

        final Function<Stream<SchemaPairCompatibility>, List<AvroIncompatibility>>
                flatMapToIncompat = inputStream -> inputStream.
                        map(this::toAvroIncompatibility).
                        flatMap(Collection::stream).collect(Collectors.toList());

        switch (compatibilityMode) {
            case FORWARD:
                return forwardCompatibleFn.andThen(this::toAvroIncompatibility).
                        apply(getLastSchema.get(), newSchema);

            case COMPATIBLE:
                return flatMapToIncompat.apply(
                        fullyCompatibleFn.apply(getLastSchema.get(), newSchema).stream()
                );
            case NONE:
                return Collections.emptyList();
            default:
                throw new UnsupportedOperationException("Unsupported compatibility mode "+ compatibilityMode
                        + " supplied for avro compatibility validation");
        }

    }

    private List<AvroIncompatibility> toAvroIncompatibility(final SchemaPairCompatibility schemaPairCompatibility) {
        final Function<Incompatibility, AvroIncompatibility> mapperFn = (incompat) ->
                new AvroIncompatibility(incompat.getLocation(), incompat.getMessage(), incompat.getType());

        return schemaPairCompatibility.getResult().getIncompatibilities().stream().
                map(mapperFn).collect(Collectors.toList());
    }

    public static class AvroIncompatibility {
        private final String location;
        private final String message;
        private final SchemaCompatibility.SchemaIncompatibilityType incompatibilityType;

        public AvroIncompatibility(final String location, final String message,
                                   final SchemaCompatibility.SchemaIncompatibilityType mType) {
            this.location = location;
            this.message = message;
            this.incompatibilityType = mType;
        }

        public String getLocation() {
            return location;
        }

        public SchemaCompatibility.SchemaIncompatibilityType getIncompatibilityType() {
            return incompatibilityType;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final AvroIncompatibility that = (AvroIncompatibility) o;
            return Objects.equals(location, that.location) &&
                    Objects.equals(message, that.message) && incompatibilityType == that.incompatibilityType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(location, message, incompatibilityType);
        }

        @Override
        public String toString() {
            return String.format("{ type:%s, location:%s, message:%s }", incompatibilityType,
                    location, message);
        }


    }
}


