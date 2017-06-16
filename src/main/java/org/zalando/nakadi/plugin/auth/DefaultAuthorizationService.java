package org.zalando.nakadi.plugin.auth;

import org.json.JSONObject;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

public class DefaultAuthorizationService implements AuthorizationService {

    @Override
    public boolean isAuthorized(String token, Operation operation, JSONObject eventType) {
        return true;
    }

    @Override
    public boolean isAuthorizationAttributeValid(final AuthorizationAttribute authorizationAttribute) {
        return true;
    }
}
