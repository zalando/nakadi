package org.zalando.nakadi.util;

import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class GenericRecordWrapper {
    private final GenericRecord genericRecord;

    public GenericRecordWrapper(final GenericRecord genericRecord) {
        this.genericRecord = genericRecord;
    }

    public String getString(final String key) {
        return Optional.ofNullable(genericRecord.get(key))
                .map(Object::toString)
                .orElse(null);
    }

    public List<String> getListOfStrings(final String key) {
        return Optional.ofNullable((String[]) genericRecord.get(key))
                .map(Arrays::asList)
                .orElse(null);
    }

    public Long getLong(final String key) {
        return (Long) genericRecord.get(key);
    }

}
