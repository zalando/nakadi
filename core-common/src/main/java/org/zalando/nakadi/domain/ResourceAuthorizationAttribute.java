package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;
import java.util.Objects;

@Immutable
public class ResourceAuthorizationAttribute implements AuthorizationAttribute {

    @NotNull
    private final String dataType;

    @NotNull
    private final String value;

    public ResourceAuthorizationAttribute(@JsonProperty("data_type") final String dataType,
                                          @JsonProperty("value") final String value) {
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
        final ResourceAuthorizationAttribute that = (ResourceAuthorizationAttribute) o;
        return Objects.equals(dataType, that.dataType) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = dataType != null ? dataType.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ResourceAuthorizationAttribute{" +
                "dataType='" + dataType + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
