package org.zalando.nakadi.plugin.auth.attribute;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;

public class TeamAuthorizationAttribute extends SimpleAuthorizationAttribute {
    private static final String TEAMS_AUTHORIZATION_ATTRIBUTE_TYPE = "team";

    public TeamAuthorizationAttribute(final String value) {
        super(TEAMS_AUTHORIZATION_ATTRIBUTE_TYPE, value);
    }

    public static boolean isTeamAuthorizationAttribute(final AuthorizationAttribute attribute) {
        return TEAMS_AUTHORIZATION_ATTRIBUTE_TYPE.equals(attribute.getDataType());
    }

}
