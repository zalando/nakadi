package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

@Immutable
public class EventTypeAuthorizationAttribute implements AuthorizationAttribute {

    private final String dataType;
    private final String value;

    public EventTypeAuthorizationAttribute(@JsonProperty("data_type") final String dataType,
                                           @JsonProperty("value") final String value) {
        this.dataType = dataType;
        this.value = value;
    }

    @NotNull
    @Override
    public String getDataType() {
        return dataType;
    }

    @NotNull
    @Override
    public String getValue() {
        return value;
    }

}
