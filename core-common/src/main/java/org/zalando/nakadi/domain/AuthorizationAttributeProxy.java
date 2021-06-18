package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;

public class AuthorizationAttributeProxy implements AuthorizationAttribute {
    private final EventOwnerHeader ownerHeader;

    public AuthorizationAttributeProxy(final EventOwnerHeader ownerHeader) {
        this.ownerHeader = ownerHeader;
    }

    @Override
    public String getDataType() {
        return ownerHeader.getName();
    }

    @Override
    public String getValue() {
        return ownerHeader.getValue();
    }
}
