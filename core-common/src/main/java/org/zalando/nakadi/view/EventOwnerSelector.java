package org.zalando.nakadi.view;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Objects;

public class EventOwnerSelector {
    public enum Type {
        PATH,
        STATIC,
        METADATA,
    }

    @NotNull
    private Type type;

    @NotNull
    @Size(min = 1, message = "the length of the name must be >= 1")
    private String name;

    @NotNull
    private String value;

    public Type getType() {
        return type;
    }

    public void setType(final Type type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    public EventOwnerSelector() {
    }

    public EventOwnerSelector(final Type type, final String name, final String value) {
        this.type = type;
        this.name = name;
        this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final EventOwnerSelector that = (EventOwnerSelector) o;
        return Objects.equals(this.type, that.type) && Objects.equals(this.name, that.name)
                && Objects.equals(this.value, that.value);
    }

    @Override
    public String toString() {
        return "{ " + "type: " + this.type + ", name: " + this.name + ", value: " + this.value + " }";
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.type, this.value);
    }
}
