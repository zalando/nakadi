package org.zalando.nakadi.service;

import org.junit.Test;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.domain.AdminAuthorization;
import org.zalando.nakadi.domain.AdminAuthorizationAttribute;
import org.zalando.nakadi.domain.Permission;
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
    private final TransactionTemplate transactionTemplate;

    public AdminServiceTest() {
        this.authorizationDbRepository = mock(AuthorizationDbRepository.class);
        this.transactionTemplate = mock(TransactionTemplate.class);
        this.adminService = new AdminService(authorizationDbRepository, transactionTemplate);
    }

    @Test
    public void whenUpdateAdminsThenOk() {
        final AuthorizationAttribute user1 = new AdminAuthorizationAttribute("user", "user1");
        final AuthorizationAttribute user2 = new AdminAuthorizationAttribute("user", "user2");
        final AuthorizationAttribute service2 = new AdminAuthorizationAttribute("service", "service2");

        final List<AuthorizationAttribute> newAttrs = Arrays.asList(user1, user2, service2);

        final Permission permAdminUser1 = new Permission("nakadi", AuthorizationService.Operation.ADMIN,
                new AdminAuthorizationAttribute("user", "user1"));
        final Permission permAdminService1 = new Permission("nakadi", AuthorizationService.Operation.ADMIN,
                new AdminAuthorizationAttribute("service", "service1"));
        final Permission permAdminService2 = new Permission("nakadi", AuthorizationService.Operation.ADMIN,
                new AdminAuthorizationAttribute("service", "service2"));

        final Permission permReadUser1 = new Permission("nakadi", AuthorizationService.Operation.READ,
                new AdminAuthorizationAttribute("user", "user1"));
        final Permission permReadService1 = new Permission("nakadi", AuthorizationService.Operation.READ,
                new AdminAuthorizationAttribute("service", "service1"));
        final Permission permReadService2 = new Permission("nakadi", AuthorizationService.Operation.READ,
                new AdminAuthorizationAttribute("service", "service2"));

        final Permission permWriteUser1 = new Permission("nakadi", AuthorizationService.Operation.WRITE,
                new AdminAuthorizationAttribute("user", "user1"));
        final Permission permWriteService1 = new Permission("nakadi", AuthorizationService.Operation.WRITE,
                new AdminAuthorizationAttribute("service", "service1"));
        final Permission permWriteService2 = new Permission("nakadi", AuthorizationService.Operation.WRITE,
                new AdminAuthorizationAttribute("service", "service2"));
        final AdminAuthorization newAuthz = new AdminAuthorization(newAttrs, newAttrs, newAttrs);

        when(authorizationDbRepository.listAdmins()).thenReturn(Arrays.asList(permAdminUser1, permAdminService1,
                permAdminService2, permReadUser1, permReadService1, permReadService2, permWriteUser1,
                permWriteService1, permWriteService2));
        adminService.updateAdmins(newAuthz);
    }
}
