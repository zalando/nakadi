package org.zalando.nakadi.service;


import com.google.common.collect.ImmutableList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.zalando.nakadi.domain.EventTypeAuthorization;
import org.zalando.nakadi.domain.EventTypeAuthorizationAttribute;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.plugin.api.PluginException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

public class AuthorizationValidatorTest {

    private final AuthorizationValidator validator;
    private final AuthorizationService authorizationService;

    private final AuthorizationAttribute attr1 = new EventTypeAuthorizationAttribute("type1", "value1");
    private final AuthorizationAttribute attr2 = new EventTypeAuthorizationAttribute("type2", "value2");
    private final AuthorizationAttribute attr3 = new EventTypeAuthorizationAttribute("type3", "value3");
    private final AuthorizationAttribute attr4 = new EventTypeAuthorizationAttribute("type4", "value4");

    public AuthorizationValidatorTest() {
        authorizationService = mock(AuthorizationService.class);

        validator = new AuthorizationValidator(authorizationService, mock(EventTypeRepository.class));
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
        } catch (final UnableProcessException e) {
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
        } catch (final UnableProcessException e) {
            assertThat(e.getMessage(), equalTo(
                    "authorization property 'admins' contains duplicated attribute(s): type1:value1, type3:value3; " +
                            "authorization property 'readers' contains duplicated attribute(s): type2:value2"));
        }
    }

    @Test(expected = ServiceTemporarilyUnavailableException.class)
    public void whenPluginExceptionInIsAuthorizationAttributeValidThenServiceUnavailableException() throws Exception {

        final EventTypeAuthorization auth = new EventTypeAuthorization(
                ImmutableList.of(attr1),
                ImmutableList.of(attr2),
                ImmutableList.of(attr3));

        when(authorizationService.isAuthorizationAttributeValid(any())).thenThrow(new PluginException("blah"));

        validator.validateAuthorization(auth);
    }

    @Test
    public void whenAuthorizationIsNullWhileUpdatingETThenOk() throws Exception {
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder().authorization(null).build());
    }

    @Test(expected = AccessDeniedException.class)
    public void whenNotAuthorizedThenForbiddenAccessException() throws Exception {
        when(authorizationService.isAuthorized(any(), any())).thenReturn(false);
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder()
                .authorization(new EventTypeAuthorization(null, null, null)).build());
    }

    @Test
    public void whenAuthorizedThenOk() throws Exception {
        when(authorizationService.isAuthorized(any(), any())).thenReturn(true);
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder()
                .authorization(new EventTypeAuthorization(null, null, null)).build());
    }

    @Test(expected = ServiceTemporarilyUnavailableException.class)
    public void whenPluginExceptionInAuthorizeEventTypeUpdateThenServiceTemporaryUnavailableException()
            throws Exception {
        when(authorizationService.isAuthorized(any(), any())).thenThrow(new PluginException("blah"));
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder()
                .authorization(new EventTypeAuthorization(null, null, null)).build());
    }

}
