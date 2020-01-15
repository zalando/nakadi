package org.zalando.nakadi.domain;

import javax.validation.constraints.NotNull;

public class EventAuthField {
    @NotNull
    private String type;

    @NotNull
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
        return (this.type.equals(that.type) && this.path.equals(that.path));
    }

    @Override
    public String toString() {
        return "{ " + "type: " + this.type + ", path: " + this.path + " }";
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }
}
