package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.domain.ResourceAuthorizationAttribute;
import org.zalando.nakadi.domain.ResourceImpl;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ForbiddenOperationException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.plugin.api.PluginException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.exceptions.AuthorizationInvalidException;
import org.zalando.nakadi.plugin.api.exceptions.OperationOnResourceNotPermitedException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthorizationValidatorTest {

    private final AuthorizationValidator validator;
    private final AuthorizationService authorizationService;
    private final AdminService adminService;

    private final AuthorizationAttribute attr1 = new ResourceAuthorizationAttribute("type1", "value1");
    private final AuthorizationAttribute attr2 = new ResourceAuthorizationAttribute("type2", "value2");
    private final AuthorizationAttribute attr3 = new ResourceAuthorizationAttribute("type3", "value3");
    private final AuthorizationAttribute attr4 = new ResourceAuthorizationAttribute("type4", "value4");

    public AuthorizationValidatorTest() {
        authorizationService = mock(AuthorizationService.class);
        adminService = mock(AdminService.class);

        validator = new AuthorizationValidator(authorizationService,
                mock(EventTypeRepository.class), adminService);
    }

    @Test
    public void whenInvalidAuthAttributesThenInvalidEventTypeException() {

        final ResourceAuthorization auth = new ResourceAuthorization(
                ImmutableList.of(attr1), ImmutableList.of(attr2), ImmutableList.of(attr3, attr4));

        final Resource resource = new ResourceImpl("myResource1", "event-type", auth, null);
        when(authorizationService.isAuthorizationAttributeValid(attr1)).thenReturn(false);
        when(authorizationService.isAuthorizationAttributeValid(attr2)).thenReturn(true);
        when(authorizationService.isAuthorizationAttributeValid(attr3)).thenReturn(true);
        when(authorizationService.isAuthorizationAttributeValid(attr4)).thenReturn(false);
        try {
            validator.validateAuthorization(resource);
            fail("Exception expected to be thrown");
        } catch (final UnableProcessException e) {
            assertThat(e.getMessage(), equalTo("authorization attribute type1:value1 is invalid, " +
                    "authorization attribute type4:value4 is invalid"));
        }
    }

    @Test
    public void whenDuplicatesThenInvalidEventTypeException() {

        final ResourceAuthorization auth = new ResourceAuthorization(
                ImmutableList.of(attr1, attr3, attr2, attr1, attr1, attr3),
                ImmutableList.of(attr3, attr2, attr2),
                ImmutableList.of(attr3, attr4));

        when(authorizationService.isAuthorizationAttributeValid(any())).thenReturn(true);
        final Resource resource = new ResourceImpl("myResource1", "event-type", auth, null);

        try {
            validator.validateAuthorization(resource);
            fail("Exception expected to be thrown");
        } catch (final UnableProcessException e) {
            assertThat(e.getMessage(), equalTo(
                    "authorization property 'admins' contains duplicated attribute(s): type1:value1, type3:value3; " +
                            "authorization property 'readers' contains duplicated attribute(s): type2:value2"));
        }
    }

    @Test(expected = ForbiddenOperationException.class)
    public void whenOperationOnResourceNotPermittedExceptionThenForbiddenOperationException() {

        final ResourceAuthorization auth = new ResourceAuthorization(
                ImmutableList.of(attr1),
                ImmutableList.of(attr2),
                ImmutableList.of(attr3));
        when(authorizationService.isAuthorizationAttributeValid(any())).thenReturn(true);
        doThrow(new OperationOnResourceNotPermitedException("blah"))
                .when(authorizationService).areAllAuthorizationsForResourceValid(any());
        final Resource resource = new ResourceImpl("myResource1", "event-type", auth, null);
        validator.validateAuthorization(resource);
    }

    @Test(expected = UnableProcessException.class)
    public void whenAuthorizationInvalidExceptionThenUnableProcessException() {

        final ResourceAuthorization auth = new ResourceAuthorization(
                ImmutableList.of(attr1),
                ImmutableList.of(attr2),
                ImmutableList.of(attr3));
        when(authorizationService.isAuthorizationAttributeValid(any())).thenReturn(true);
        doThrow(new AuthorizationInvalidException("blah"))
                .when(authorizationService).areAllAuthorizationsForResourceValid(any());
        final Resource resource = new ResourceImpl("myResource1", "event-type", auth, null);
        validator.validateAuthorization(resource);
    }

    @Test(expected = ServiceTemporarilyUnavailableException.class)
    public void whenPluginExceptionInIsAuthorizationAttributeValidThenServiceUnavailableException() {

        final ResourceAuthorization auth = new ResourceAuthorization(
                ImmutableList.of(attr1),
                ImmutableList.of(attr2),
                ImmutableList.of(attr3));

        when(authorizationService.isAuthorizationAttributeValid(any())).thenThrow(new PluginException("blah"));
        final Resource resource = new ResourceImpl("myResource1", "event-type", auth, null);
        validator.validateAuthorization(resource);
    }

    @Test
    public void whenAuthorizationIsNullWhileUpdatingETThenOk() {
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder().authorization(null).build());
    }

    @Test(expected = AccessDeniedException.class)
    public void whenNotAuthorizedThenForbiddenAccessException() {
        when(authorizationService.isAuthorized(any(), any())).thenReturn(false);
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder()
                .authorization(new ResourceAuthorization(null, null, null)).build());
    }

    @Test
    public void whenETAdminNotAuthorizedButAdminThenOk() {
        when(authorizationService.isAuthorized(any(), any())).thenReturn(false);
        when(adminService.isAdmin(any())).thenReturn(true);
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder()
                .authorization(new ResourceAuthorization(null, null, null)).build());
    }

    @Test
    public void whenAuthorizedThenOk() {
        when(authorizationService.isAuthorized(any(), any())).thenReturn(true);
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder()
                .authorization(new ResourceAuthorization(null, null, null)).build());
    }

    @Test(expected = ServiceTemporarilyUnavailableException.class)
    public void whenPluginExceptionInAuthorizeEventTypeUpdateThenServiceTemporarilyUnavailableException() {
        when(authorizationService.isAuthorized(any(), any())).thenThrow(new PluginException("blah"));
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder()
                .authorization(new ResourceAuthorization(null, null, null)).build());
    }
}
