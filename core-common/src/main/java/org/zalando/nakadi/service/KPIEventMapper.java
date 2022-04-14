package org.zalando.nakadi.service;

import com.google.common.annotations.VisibleForTesting;
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
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KPIEventMapper {
    private static final Set<Class<?>> BOOLEAN_TYPES = Set.of(boolean.class, Boolean.class);
    private static final Set<Class<?>> SUPPORTED_FIELD_TYPES = Set.of(
            int.class, Integer.class,
            long.class, Long.class,
            float.class, Float.class,
            double.class, Double.class,
            boolean.class, Boolean.class,
            String.class);

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

        // Take all methods that are public and has zero parameters, and map from method name to method.
        final var getterCandidates = Arrays.stream(mappedClass.getMethods())
                .filter(method -> method.getParameterCount() == 0)
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .collect(Collectors.toMap(Method::getName, Function.identity()));

        return Arrays.stream(mappedClass.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(KPIEvent.KPIField.class))
                .map(field -> mapFieldToKPIGetter(getterCandidates, field))
                .collect(Collectors.toSet());
    }

    private KPIGetter mapFieldToKPIGetter(final Map<String, Method> getterCandidates, final Field field) {
        if (!SUPPORTED_FIELD_TYPES.contains(field.getType())) {
            throw new InternalNakadiException("Type of " + field.getDeclaringClass().getName() + "." + field.getName()
                    + " is not supported. Supported types are : " + SUPPORTED_FIELD_TYPES);
        }
        final var kpiFieldAnnotation = field.getAnnotation(KPIEvent.KPIField.class);
        final Method getter;
        if (kpiFieldAnnotation.getter().isEmpty()) {
            final var fieldName = field.getName();
            final var partialGetterName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
            final var getName = "get" + partialGetterName;
            final var isName = "is" + partialGetterName;
            if (getterCandidates.containsKey(getName)) {
                getter = getterCandidates.get(getName);
            } else if (BOOLEAN_TYPES.contains(field.getType()) && getterCandidates.containsKey(isName)) {
                getter = getterCandidates.get(isName);
            } else {
                getter = null;
            }
        } else {
            getter = getterCandidates.get(kpiFieldAnnotation.getter());
        }
        if (getter == null) {
            throw new InternalNakadiException("No getter found for " + field.getDeclaringClass().getName() + "."
                    + field.getName());
        }
        return new KPIGetter(kpiFieldAnnotation.value(), getter);
    }

    public GenericRecord mapToGenericRecord(final KPIEvent kpiEvent, final Schema schema) {
        final var recordBuilder = new GenericRecordBuilder(schema);
        gettersOf(kpiEvent.getClass()).forEach(kpiGetter -> {
            try {
                recordBuilder.set(kpiGetter.name, kpiGetter.getter.invoke(kpiEvent));
            } catch (final IllegalAccessException | InvocationTargetException | AvroRuntimeException ex) {
                throw new InternalNakadiException("Cannot map KPIEvent " + kpiEvent.getClass().getName()
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
                throw new InternalNakadiException("Cannot map KPIEvent " + kpiEvent.getClass().getName()
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
