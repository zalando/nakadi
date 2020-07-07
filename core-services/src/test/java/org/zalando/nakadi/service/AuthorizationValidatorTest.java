package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.domain.ResourceAuthorizationAttribute;
import org.zalando.nakadi.domain.ResourceImpl;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ForbiddenOperationException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.UnprocessableEntityException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.exceptions.AuthorizationInvalidException;
import org.zalando.nakadi.plugin.api.exceptions.OperationOnResourceNotPermittedException;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

public class AuthorizationValidatorTest {

    private final AuthorizationValidator validator;
    private final AuthorizationService authorizationService;
    private final AdminService adminService;

    private final AuthorizationAttribute attr1 = new ResourceAuthorizationAttribute("type1", "value1");
    private final AuthorizationAttribute attr2 = new ResourceAuthorizationAttribute("type2", "value2");
    private final AuthorizationAttribute attr3 = new ResourceAuthorizationAttribute("type3", "value3");
    private final AuthorizationAttribute attr4 = new ResourceAuthorizationAttribute("type4", "value4");

    public AuthorizationValidatorTest() {
        authorizationService = Mockito.mock(AuthorizationService.class);
        adminService = Mockito.mock(AdminService.class);

        validator = new AuthorizationValidator(authorizationService,
                Mockito.mock(EventTypeCache.class), adminService);
    }

    @Test
    public void whenInvalidAuthAttributesThenInvalidEventTypeException() {

        final ResourceAuthorization auth = new ResourceAuthorization(
                ImmutableList.of(attr1), ImmutableList.of(attr2), ImmutableList.of(attr3, attr4));

        final Resource resource = new ResourceImpl("myResource1", "event-type", auth, null);
        Mockito.doThrow(new AuthorizationInvalidException("some attributes are not ok"))
                .when(authorizationService).isAuthorizationForResourceValid(any());
        try {
            validator.validateAuthorization(resource);
            fail("Exception expected to be thrown");
        } catch (final UnprocessableEntityException e) {
            assertThat(e.getMessage(), equalTo("some attributes are not ok"));
        }
    }

    @Test
    public void whenDuplicatesThenInvalidEventTypeException() {

        final ResourceAuthorization auth = new ResourceAuthorization(
                ImmutableList.of(attr1, attr3, attr2, attr1, attr1, attr3),
                ImmutableList.of(attr3, attr2, attr2),
                ImmutableList.of(attr3, attr4));

        final Resource resource = new ResourceImpl("myResource1", "event-type", auth, null);

        try {
            validator.validateAuthorization(resource);
            fail("Exception expected to be thrown");
        } catch (final UnableProcessException e) {
            assertThat(e.getMessage(), equalTo(
                    "authorization property 'ADMIN' contains duplicated attribute(s): type1:value1, type3:value3; " +
                            "authorization property 'READ' contains duplicated attribute(s): type2:value2"));
        }
    }

    @Test(expected = ForbiddenOperationException.class)
    public void whenOperationOnResourceNotPermittedExceptionThenForbiddenOperationException() {

        final ResourceAuthorization auth = new ResourceAuthorization(
                ImmutableList.of(attr1),
                ImmutableList.of(attr2),
                ImmutableList.of(attr3));
        Mockito.doThrow(new OperationOnResourceNotPermittedException("blah"))
                .when(authorizationService).isAuthorizationForResourceValid(any());
        final Resource resource = new ResourceImpl("myResource1", "event-type", auth, null);
        validator.validateAuthorization(resource);
    }

    @Test(expected = UnprocessableEntityException.class)
    public void whenAuthorizationInvalidExceptionThenUnableProcessException() {

        final ResourceAuthorization auth = new ResourceAuthorization(
                ImmutableList.of(attr1),
                ImmutableList.of(attr2),
                ImmutableList.of(attr3));
        Mockito.doThrow(new AuthorizationInvalidException("blah"))
                .when(authorizationService).isAuthorizationForResourceValid(any());
        final Resource resource = new ResourceImpl("myResource1", "event-type", auth, null);
        validator.validateAuthorization(resource);
    }

    @Test(expected = ServiceTemporarilyUnavailableException.class)
    public void whenPluginExceptionInIsAuthorizationAttributeValidThenServiceUnavailableException() {

        final ResourceAuthorization auth = new ResourceAuthorization(
                ImmutableList.of(attr1),
                ImmutableList.of(attr2),
                ImmutableList.of(attr3));
        Mockito.doThrow(new PluginException("blah"))
                .when(authorizationService).isAuthorizationForResourceValid(any());
        final Resource resource = new ResourceImpl("myResource1", "event-type", auth, null);
        validator.validateAuthorization(resource);
    }

    @Test
    public void whenAuthorizationIsNullWhileUpdatingETThenOk() {
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder().authorization(null).build());
    }

    @Test(expected = AccessDeniedException.class)
    public void whenNotAuthorizedThenForbiddenAccessException() {
        Mockito.when(authorizationService.isAuthorized(any(), any())).thenReturn(false);
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder()
                .authorization(new ResourceAuthorization(null, null, null)).build());
    }

    @Test
    public void whenETAdminNotAuthorizedButAdminThenOk() {
        Mockito.when(authorizationService.isAuthorized(any(), any())).thenReturn(false);
        Mockito.when(adminService.isAdmin(any())).thenReturn(true);
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder()
                .authorization(new ResourceAuthorization(null, null, null)).build());
    }

    @Test
    public void whenAuthorizedThenOk() {
        Mockito.when(authorizationService.isAuthorized(any(), any())).thenReturn(true);
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder()
                .authorization(new ResourceAuthorization(null, null, null)).build());
    }

    @Test(expected = ServiceTemporarilyUnavailableException.class)
    public void whenPluginExceptionInAuthorizeEventTypeUpdateThenServiceTemporarilyUnavailableException() {
        Mockito.when(authorizationService.isAuthorized(any(), any())).thenThrow(new PluginException("blah"));
        validator.authorizeEventTypeAdmin(EventTypeTestBuilder.builder()
                .authorization(new ResourceAuthorization(null, null, null)).build());
    }

    @Test
    public void whenBatchItemWithNoOwnerThenNotChecked() {
        final BatchItem item = new BatchItem("{}", null, null, null);
        item.setOwner(null);
        Mockito.when(authorizationService.isAuthorized(any(), any())).thenReturn(true);
        validator.authorizeEventWrite(item);
        Mockito.verify(authorizationService, Mockito.times(0)).isAuthorized(any(), any());
    }

    @Test
    public void whenBatchItemWithOwnerThenAuthorizationChecked() {
        final BatchItem item = new BatchItem("{}", null, null, null);
        item.setOwner(new EventOwnerHeader("retailer", "nakadi"));
        Mockito.when(authorizationService.isAuthorized(any(), any())).thenReturn(true);
        validator.authorizeEventWrite(item);
        Mockito.verify(authorizationService, Mockito.times(1)).isAuthorized(any(), any());
    }
}
