package org.zalando.nakadi.service;

import org.junit.Test;
import org.zalando.nakadi.domain.Permission;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.domain.ResourceAuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.db.AuthorizationDbRepository;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AdminServiceTest {

    private final AuthorizationDbRepository authorizationDbRepository;
    private final AdminService adminService;
    private final AuthorizationService authorizationService;

    public AdminServiceTest() {
        this.authorizationDbRepository = mock(AuthorizationDbRepository.class);
        this.authorizationService = mock(AuthorizationService.class);
        this.adminService = new AdminService(authorizationDbRepository, authorizationService);
    }

    @Test
    public void whenUpdateAdminsThenOk() {
        final AuthorizationAttribute user1 = new ResourceAuthorizationAttribute("user", "user1");
        final AuthorizationAttribute user2 = new ResourceAuthorizationAttribute("user", "user2");
        final AuthorizationAttribute service2 = new ResourceAuthorizationAttribute("service", "service2");

        final List<AuthorizationAttribute> newAttrs = Arrays.asList(user1, user2, service2);

        final Permission permAdminUser1 = new Permission("nakadi", AuthorizationService.Operation.ADMIN,
                new ResourceAuthorizationAttribute("user", "user1"));
        final Permission permAdminService1 = new Permission("nakadi", AuthorizationService.Operation.ADMIN,
                new ResourceAuthorizationAttribute("service", "service1"));
        final Permission permAdminService2 = new Permission("nakadi", AuthorizationService.Operation.ADMIN,
                new ResourceAuthorizationAttribute("service", "service2"));

        final Permission permReadUser1 = new Permission("nakadi", AuthorizationService.Operation.READ,
                new ResourceAuthorizationAttribute("user", "user1"));
        final Permission permReadService1 = new Permission("nakadi", AuthorizationService.Operation.READ,
                new ResourceAuthorizationAttribute("service", "service1"));
        final Permission permReadService2 = new Permission("nakadi", AuthorizationService.Operation.READ,
                new ResourceAuthorizationAttribute("service", "service2"));

        final Permission permWriteUser1 = new Permission("nakadi", AuthorizationService.Operation.WRITE,
                new ResourceAuthorizationAttribute("user", "user1"));
        final Permission permWriteService1 = new Permission("nakadi", AuthorizationService.Operation.WRITE,
                new ResourceAuthorizationAttribute("service", "service1"));
        final Permission permWriteService2 = new Permission("nakadi", AuthorizationService.Operation.WRITE,
                new ResourceAuthorizationAttribute("service", "service2"));
        final ResourceAuthorization newAuthz = new ResourceAuthorization(newAttrs, newAttrs, newAttrs);

        when(authorizationDbRepository.listAdmins()).thenReturn(Arrays.asList(permAdminUser1, permAdminService1,
                permAdminService2, permReadUser1, permReadService1, permReadService2, permWriteUser1,
                permWriteService1, permWriteService2));
        adminService.updateAdmins(newAuthz.toPermissionsList("nakadi"));
    }
}
