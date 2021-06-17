package org.zalando.nakadi.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.zalando.nakadi.domain.ResourceAuthorizationAttribute;

import java.beans.PropertyEditorSupport;

public class AuthorizationAttributeQueryParser extends PropertyEditorSupport {

    @Override
    public void setAsText(String text) throws IllegalArgumentException {
        if (StringUtils.isEmpty(text)) {
            throw new IllegalArgumentException("Authorization query is empty");
        } else {
            String[] authorizationQuery = text.split(":");
            if (authorizationQuery.length != 2) {
                throw new IllegalArgumentException("Authorization format is incorrect. Should be data_type:data_value");
            }
            ResourceAuthorizationAttribute authorizationAttribute =
                    new ResourceAuthorizationAttribute(authorizationQuery[0], authorizationQuery[1]);
            setValue(authorizationAttribute);
        }
    }
}
