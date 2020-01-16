package org.zalando.nakadi.domain;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Objects;

public class EventAuthField {
    @NotNull
    @Size(min = 1, message = "the length of the type must be >= 1")
    private String type;

    @NotNull
    @Size(min = 1, message = "the length of the path must be >= 1")
    private String path;

    public String getType() {
        return type;
    }

    public String getPath() {
        return path;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public void setPath(final String path) {
        this.path = path;
    }

    public EventAuthField() {
    }

    public EventAuthField(final String path, final String type) {
        this.type = type;
        this.path = path;
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
        final EventAuthField that = (EventAuthField) o;
        return Objects.equals(this.type, that.type) && Objects.equals(this.path, that.path);
    }

    @Override
    public String toString() {
        return "{ " + "type: " + this.type + ", path: " + this.path + " }";
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.path);
    }
}
