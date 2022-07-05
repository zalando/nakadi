package org.zalando.nakadi.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.zalando.nakadi.domain.CompatibilityMode;

import java.util.Objects;

public class CompatibilityResponse {
    private boolean compatible;
    private CompatibilityMode compatibilityMode;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String error;
    private String validatedAgainstVersion;

    public CompatibilityResponse(final boolean compatible, final CompatibilityMode compatibilityMode,
                                 final String error,
                                 final String validatedAgainstVersion) {
        this.compatible = compatible;
        this.compatibilityMode = compatibilityMode;
        this.error = error;
        this.validatedAgainstVersion = validatedAgainstVersion;
    }

    public boolean isCompatible() {
        return compatible;
    }

    public CompatibilityMode getCompatibilityMode() {
        return compatibilityMode;
    }

    public String getError() {
        return error;
    }

    public String getValidatedAgainstVersion() {
        return validatedAgainstVersion;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }
        final CompatibilityResponse that = (CompatibilityResponse) o;
        return compatible == that.compatible && compatibilityMode == that.compatibilityMode
                && Objects.equals(error, that.error)
                && Objects.equals(validatedAgainstVersion, that.validatedAgainstVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(compatible, compatibilityMode, error, validatedAgainstVersion);
    }

    @Override
    public String toString() {
        return "CompatibilityResponse{" +
                "compatible=" + compatible +
                ", compatibilityMode=" + compatibilityMode +
                ", error='" + error + '\'' +
                ", validatedAgainstVersion='" + validatedAgainstVersion + '\'' +
                '}';
    }
}
