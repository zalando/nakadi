package org.zalando.nakadi.util;

import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;

public class AuthorizationUtils {
    public static String errorMessage(final AccessDeniedException e) {
        return new StringBuilder()
                .append("Access on ")
                .append(e.getOperation())
                .append(" ")
                .append(e.getResource().getType())
                .append(":")
                .append(e.getResource().getName())
                .append(" denied")
                .toString();
    }
}
