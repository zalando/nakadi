package org.zalando.nakadi.plugin.auth.attribute;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;

import java.util.Objects;

public class SimpleAuthorizationAttribute implements AuthorizationAttribute {

    private final String dataType;
    private final String value;

    public SimpleAuthorizationAttribute(final String dataType, final String value) {
        this.dataType = dataType;
        this.value = value;
    }

    @Override
    public String getDataType() {
        return dataType;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SimpleAuthorizationAttribute that = (SimpleAuthorizationAttribute) o;
        return Objects.equals(dataType, that.dataType) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataType, value);
    }

    @Override
    public String toString() {
        return "SimpleAuthorizationAttribute{" +
                "dataType='" + dataType + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
