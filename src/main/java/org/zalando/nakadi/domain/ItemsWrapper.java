package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import javax.annotation.concurrent.Immutable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;

@Immutable
@Getter
public class ItemsWrapper<T> {

    @Valid
    @NotNull
    private final List<T> items;

    @JsonCreator
    public ItemsWrapper(@JsonProperty("items") final List<T> items) {
        this.items = items;
    }
}
