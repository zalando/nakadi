package org.zalando.nakadi.service;


import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.zalando.nakadi.domain.EventTypeAuthorization;
import org.zalando.nakadi.domain.EventTypeAuthorizationAttribute;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.plugin.api.PluginException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthorizationValidatorTest {

    private final AuthorizationValidator validator;
    private final AuthorizationService authorizationService;

    private final AuthorizationAttribute attr1 = new EventTypeAuthorizationAttribute("type1", "value1");;
    private final AuthorizationAttribute attr2 = new EventTypeAuthorizationAttribute("type2", "value2");;
    private final AuthorizationAttribute attr3 = new EventTypeAuthorizationAttribute("type3", "value3");;
    private final AuthorizationAttribute attr4 = new EventTypeAuthorizationAttribute("type4", "value4");;

    public AuthorizationValidatorTest() {
        authorizationService = mock(AuthorizationService.class);
        validator = new AuthorizationValidator(authorizationService);
    }

    @Test
    public void whenInvalidAuthAttributesThenInvalidEventTypeException() throws Exception {

        final EventTypeAuthorization auth = new EventTypeAuthorization(
                ImmutableList.of(attr1), ImmutableList.of(attr2), ImmutableList.of(attr3, attr4));

        when(authorizationService.isAuthorizationAttributeValid(attr1)).thenReturn(false);
        when(authorizationService.isAuthorizationAttributeValid(attr2)).thenReturn(true);
        when(authorizationService.isAuthorizationAttributeValid(attr3)).thenReturn(true);
        when(authorizationService.isAuthorizationAttributeValid(attr4)).thenReturn(false);

        try {
            validator.validateAuthorization(auth);
            fail("Exception expected to be thrown");
        } catch (final InvalidEventTypeException e) {
            assertThat(e.getMessage(), equalTo("authorization attribute type1:value1 is invalid, " +
                    "authorization attribute type4:value4 is invalid"));
        }
    }

    @Test
    public void whenDuplicatesThenInvalidEventTypeException() throws Exception {

        final EventTypeAuthorization auth = new EventTypeAuthorization(
                ImmutableList.of(attr1, attr3, attr2, attr1, attr1, attr3),
                ImmutableList.of(attr3, attr2, attr2),
                ImmutableList.of(attr3, attr4));

        when(authorizationService.isAuthorizationAttributeValid(any())).thenReturn(true);

        try {
            validator.validateAuthorization(auth);
            fail("Exception expected to be thrown");
        } catch (final InvalidEventTypeException e) {
            assertThat(e.getMessage(), equalTo(
                    "authorization property 'admins' contains duplicated attribute(s): type1:value1, type3:value3; " +
                            "authorization property 'readers' contains duplicated attribute(s): type2:value2"));
        }
    }

    @Test(expected = ServiceUnavailableException.class)
    public void whenPluginExceptionThenServiceUnavailableException() throws Exception {

        final EventTypeAuthorization auth = new EventTypeAuthorization(
                ImmutableList.of(attr1),
                ImmutableList.of(attr2),
                ImmutableList.of(attr3));

        when(authorizationService.isAuthorizationAttributeValid(any())).thenThrow(new PluginException("blah"));

        validator.validateAuthorization(auth);
    }
}
