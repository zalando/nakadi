package org.zalando.nakadi.service;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.json.JSONObject;
import org.zalando.nakadi.domain.kpi.KPIEvent;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KPIEventMapper {

    public static class KPIEventMappingException extends NakadiBaseException {
        public KPIEventMappingException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    private static class KPIGetter {
        final String name;
        final Method getter;

        KPIGetter(final String name, final Method getter) {
            this.name = name;
            this.getter = getter;
        }
    }

    private final Map<Class<? extends KPIEvent>, Set<KPIGetter>> fieldGetterCache = new HashMap<>();

    public KPIEventMapper(final Set<Class<? extends KPIEvent>> mappedClasses) {
        for (final Class<? extends KPIEvent> mappedClass : mappedClasses) {
            fieldGetterCache.put(mappedClass, findGettersOfKPIFields(mappedClass));
        }
    }

    private Set<KPIGetter> findGettersOfKPIFields(final Class<? extends KPIEvent> mappedClass) {
        return Arrays.stream(mappedClass.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(KPIEvent.KPIField.class))
                .map(field -> findGetterByField(mappedClass, field))
                .collect(Collectors.toSet());
    }

    private KPIGetter findGetterByField(final Class<? extends KPIEvent> clazz, final Field field) {
        final var kpiField = field.getAnnotation(KPIEvent.KPIField.class);
        final String getterName;
        if (kpiField.getter().isEmpty()) {
            final var fieldName = field.getName();
            getterName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        } else {
            getterName = kpiField.getter();
        }
        try {
            final var getter = clazz.getDeclaredMethod(getterName);
            return new KPIGetter(kpiField.value(), getter);
        } catch (final NoSuchMethodException nsme) {
            throw new InternalNakadiException("No getter for " + clazz.getName() + "." + field.getName(), nsme);
        }
    }

    public GenericRecord mapToGenericRecord(final KPIEvent kpiEvent, final Schema schema) {
        final var recordBuilder = new GenericRecordBuilder(schema);
        gettersOf(kpiEvent.getClass()).forEach(kpiGetter -> {
            try {
                recordBuilder.set(kpiGetter.name, kpiGetter.getter.invoke(kpiEvent));
            } catch (final IllegalAccessException | InvocationTargetException | AvroRuntimeException ex) {
                throw new KPIEventMappingException("Cannot map KPIEvent " + kpiEvent.getClass().getName()
                        + " to GenericRecord. Unable to read " + kpiGetter.name, ex);
            }
        });
        return recordBuilder.build();
    }

    public JSONObject mapToJsonObject(final KPIEvent kpiEvent) {
        final var jsonObject = new JSONObject();
        gettersOf(kpiEvent.getClass()).forEach(kpiGetter -> {
            try {
                jsonObject.put(kpiGetter.name, kpiGetter.getter.invoke(kpiEvent));
            } catch (final IllegalAccessException | InvocationTargetException iae) {
                throw new KPIEventMappingException("Cannot map KPIEvent " + kpiEvent.getClass().getName()
                        + " to JSON. Unable to read " + kpiGetter.name, iae);
            }
        });
        return jsonObject;
    }

    private Set<KPIGetter> gettersOf(final Class<? extends KPIEvent> clazz) {
        if (this.fieldGetterCache.containsKey(clazz)) {
            return this.fieldGetterCache.get(clazz);
        }
        throw new InternalNakadiException("KPIEvent " + clazz + " is not added to KPIEventMapper");
    }
}
