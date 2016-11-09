package org.zalando.nakadi.validation.schema;

import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.validation.SchemaIncompatibility;

import java.util.ArrayList;
import java.util.List;

public class SchemaCompatibilityCheckResult {
    public boolean isCompatible() {
        return true;
    }

    public List<SchemaIncompatibility> getIncompatibilities() {
        return new ArrayList<>();
    }

    public Version getVersion() {
        return null;
    }
}
