package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

import java.util.List;

public class DefaultAuthorizationService implements AuthorizationService {

    @Override
    public boolean isAuthorized(final String token, final List<AuthorizationAttribute> attributeList) {
        return true;
    }

    @Override
    public boolean isAuthorizationAttributeValid(final AuthorizationAttribute authorizationAttribute) {
        return true;
    }
}
