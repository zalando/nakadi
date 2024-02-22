package org.zalando.nakadi.plugin.auth.utils;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.EventTypeAuthz;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.auth.attribute.SimpleAuthorizationAttribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResourceBuilder {
    private final String name;
    private final String type;
    private EventTypeAuthz resource;
    private Map<AuthorizationService.Operation, List<AuthorizationAttribute>> authSection;

    public ResourceBuilder(final String name, final String type) {
        this.name = name;
        this.type = type;
    }

    public ResourceBuilder resource(final EventTypeAuthz resource) {
        this.resource = resource;
        return this;
    }

    public ResourceBuilder add(final AuthorizationService.Operation op, final String dataType, final String value) {
        return add(op, new SimpleAuthorizationAttribute(dataType, value));
    }

    public ResourceBuilder add(final AuthorizationService.Operation op, final AuthorizationAttribute attribute) {
        if (null == authSection) {
            authSection = new HashMap<>();
        }
        if (!authSection.containsKey(op)) {
            authSection.put(op, new ArrayList<>());
        }
        authSection.get(op).add(attribute);
        return this;
    }

    public Resource build() {
        try {
            return new SimpleResource(name, type, authSection, resource);
        } finally {
            authSection = null;
        }
    }

    /**
     * Shorthand to create resource builder in tests
     */
    public static ResourceBuilder rb(final String name, final String type) {
        return new ResourceBuilder(name, type);
    }
}
