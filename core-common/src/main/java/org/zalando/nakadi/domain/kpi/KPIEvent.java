package org.zalando.nakadi.domain.kpi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.json.JSONObject;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.stream.Stream;

public abstract class KPIEvent {

    @Retention(RetentionPolicy.RUNTIME)
    public @interface KPIField {
        String value();
    }

    public static class KPIEventMappingException extends NakadiBaseException {
        public KPIEventMappingException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /*
        Implementation of this method should return the event-type name of the specific KPI event.
        This method should not be confused with the getter "getEventType()" in some specific KPI events;
        which is why this method is named "eventTypeOfThisKPIEvent()
     */
    public abstract String eventTypeOfThisKPIEvent();

    private Stream<Field> getKPIFields() {
        return Arrays.stream(this.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(KPIField.class));
    }

    public JSONObject asJsonObject() throws KPIEventMappingException {
        final var jsonObject = new JSONObject();
        getKPIFields().forEach(field -> {
            try {
                jsonObject.put(field.getAnnotation(KPIField.class).value(), field.get(this));
            } catch (final IllegalAccessException iae) {
                throw new KPIEventMappingException("Cannot map KPIEvent " + this.getClass().getName() + " to JSON."
                        + "Unable to read " + field.getName(), iae);
            }
        });
        return jsonObject;
    }

    public GenericRecord asGenericRecord(final Schema schema) {
        final var recordBuilder = new GenericRecordBuilder(schema);
        getKPIFields().forEach(field -> {
            try {
                recordBuilder.set(field.getAnnotation(KPIField.class).value(), field.get(this));
            } catch (final IllegalAccessException iae) {
                throw new KPIEventMappingException("Cannot map KPIEvent " + this.getClass().getName()
                        + "to GenericRecord. Unable to read " + field.getName(), iae);
            }
        });
        return recordBuilder.build();
    }
}
