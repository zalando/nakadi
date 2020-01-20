package org.zalando.nakadi.view;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Objects;

public class EventOwnerSelector {
    @NotNull
    @Size(min = 1, message = "the length of the type must be >= 1")
    private String type;

    @NotNull
    @Size(min = 1, message = "the length of the name must be >= 1")
    private String name;

    @NotNull
    @Size(min = 1, message = "the length of the value must be >= 1")
    private String value;

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    public EventOwnerSelector() {
    }

    public EventOwnerSelector(final String type, final String name, final String value) {
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
        if (!super.equals(o)) {
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
        return Objects.hash(this.value);
    }
}
