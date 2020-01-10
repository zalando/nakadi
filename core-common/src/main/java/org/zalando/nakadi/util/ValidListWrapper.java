package org.zalando.nakadi.util;

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.validation.Valid;
import java.util.List;

public class ValidListWrapper<T> {

    @Valid
    private List<T> list;

    @JsonCreator
    public ValidListWrapper(final List<T> list) {
        this.list = list;
    }

    public List<T> getList() {
        return list;
    }

}
