package org.zalando.nakadi.service;


import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.zalando.nakadi.domain.AdminAuthorization;
import org.zalando.nakadi.domain.AdminAuthorizationAttribute;
import org.zalando.nakadi.domain.EventTypeAuthorization;
import org.zalando.nakadi.domain.EventTypeAuthorizationAttribute;
import org.zalando.nakadi.domain.Permission;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.InsufficientAuthorizationException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.plugin.api.PluginException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.AuthorizationDbRepository;
import org.zalando.nakadi.utils.EventTypeTestBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthorizationValidatorTest {

    private final AuthorizationValidator validator;
    private final AuthorizationService authorizationService;
    private final AuthorizationDbRepository authorizationDbRepository;

    private final AuthorizationAttribute attr1 = new EventTypeAuthorizationAttribute("type1", "value1");
    private final AuthorizationAttribute attr2 = new EventTypeAuthorizationAttribute("type2", "value2");
    private final AuthorizationAttribute attr3 = new EventTypeAuthorizationAttribute("type3", "value3");
    private final AuthorizationAttribute attr4 = new EventTypeAuthorizationAttribute("type4", "value4");

    public AuthorizationValidatorTest() {
        authorizationService = mock(AuthorizationService.class);
        authorizationDbRepository = mock(AuthorizationDbRepository.class);

        validator = new AuthorizationValidator(authorizationService, mock(EventTypeRepository.class),
                authorizationDbRepository);
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

    @Test(expected = InsufficientAuthorizationException.class)
    public void whenUpdateAdminsRemovesAllAdminsThenFail() throws Exception {
        final AuthorizationAttribute user1 = new AdminAuthorizationAttribute("user", "user1");
        final AuthorizationAttribute user2 = new AdminAuthorizationAttribute("user", "user2");

        final List<AuthorizationAttribute> newAttrs = Arrays.asList(user1);

        final Permission permAdminUser1 = new Permission("nakadi", AuthorizationService.Operation.ADMIN,
                "user", "user1");
        final Permission permAdminService1 = new Permission("nakadi", AuthorizationService.Operation.ADMIN,
                "service", "service1");

        final Permission permReadUser1 = new Permission("nakadi", AuthorizationService.Operation.READ,
                "user", "user1");

        final Permission permWriteUser1 = new Permission("nakadi", AuthorizationService.Operation.WRITE,
                "user", "user1");

        final AdminAuthorization newAuthz = new AdminAuthorization(newAttrs, new ArrayList<>(), newAttrs);

        when(authorizationDbRepository.listAdmins()).thenReturn(Arrays.asList(permAdminUser1, permAdminService1,
                permReadUser1, permWriteUser1));
        validator.updateAdmins(newAuthz);
    }

    @Test
    public void whenUpdateAdminsThenOk() {
        final AuthorizationAttribute user1 = new AdminAuthorizationAttribute("user", "user1");
        final AuthorizationAttribute user2 = new AdminAuthorizationAttribute("user", "user2");
        final AuthorizationAttribute service2 = new AdminAuthorizationAttribute("service", "service2");

        final List<AuthorizationAttribute> newAttrs = Arrays.asList(user1, user2, service2);

        final Permission permAdminUser1 = new Permission("nakadi", AuthorizationService.Operation.ADMIN,
                "user", "user1");
        final Permission permAdminService1 = new Permission("nakadi", AuthorizationService.Operation.ADMIN,
                "service", "service1");
        final Permission permAdminService2 = new Permission("nakadi", AuthorizationService.Operation.ADMIN,
                "service", "service2");

        final Permission permReadUser1 = new Permission("nakadi", AuthorizationService.Operation.READ,
                "user", "user1");
        final Permission permReadService1 = new Permission("nakadi", AuthorizationService.Operation.READ,
                "service", "service1");
        final Permission permReadService2 = new Permission("nakadi", AuthorizationService.Operation.READ,
                "service", "service2");

        final Permission permWriteUser1 = new Permission("nakadi", AuthorizationService.Operation.WRITE,
                "user", "user1");
        final Permission permWriteService1 = new Permission("nakadi", AuthorizationService.Operation.WRITE,
                "service", "service1");
        final Permission permWriteService2 = new Permission("nakadi", AuthorizationService.Operation.WRITE,
                "service", "service2");
        final AdminAuthorization newAuthz = new AdminAuthorization(newAttrs, newAttrs, newAttrs);

        when(authorizationDbRepository.listAdmins()).thenReturn(Arrays.asList(permAdminUser1, permAdminService1,
                permAdminService2, permReadUser1, permReadService1, permReadService2, permWriteUser1,
                permWriteService1, permWriteService2));
        validator.updateAdmins(newAuthz);
    }

}
