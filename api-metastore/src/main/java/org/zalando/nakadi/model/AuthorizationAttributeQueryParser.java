package org.zalando.nakadi.model;

import org.apache.commons.lang.StringUtils;
import org.zalando.nakadi.domain.ResourceAuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;

import java.beans.PropertyEditorSupport;

public class AuthorizationAttributeQueryParser extends PropertyEditorSupport {

    @Override
    public void setAsText(final String text) throws IllegalArgumentException {
        if (StringUtils.isEmpty(text)) {
            throw new IllegalArgumentException("Authorization query is empty");
        } else {
            final String[] authorizationQuery = text.split(":");
            if (authorizationQuery.length != 2) {
                throw new IllegalArgumentException("Authorization format is incorrect. Should be data_type:value");
            }
            final ResourceAuthorizationAttribute authorizationAttribute =
                    new ResourceAuthorizationAttribute(authorizationQuery[0], authorizationQuery[1]);
            setValue(authorizationAttribute);
        }
    }

    public static String getQuery(final AuthorizationAttribute auth){
        return String.format("%s:%s", auth.getDataType(), auth.getValue());
    }
}
