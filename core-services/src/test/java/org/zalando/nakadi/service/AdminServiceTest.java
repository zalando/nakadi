package org.zalando.nakadi.service;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.Permission;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.domain.ResourceAuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.db.AuthorizationDbRepository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class AdminServiceTest {

    final List<Permission> defaultAdminPermissions;
    final AuthorizationAttribute user1 = new ResourceAuthorizationAttribute("user", "user1");
    final AuthorizationAttribute user2 = new ResourceAuthorizationAttribute("user", "user2");
    final AuthorizationAttribute service2 = new ResourceAuthorizationAttribute("service", "service2");
    final List<AuthorizationAttribute> newAttrs = Arrays.asList(user1, user2, service2);
    final Permission permAdminUser1 = new Permission("org/zalando/nakadi", AuthorizationService.Operation.ADMIN,
            new ResourceAuthorizationAttribute("user", "user1"));
    final Permission permAdminService1 = new Permission("org/zalando/nakadi", AuthorizationService.Operation.ADMIN,
            new ResourceAuthorizationAttribute("service", "service1"));
    final Permission permAdminService2 = new Permission("org/zalando/nakadi", AuthorizationService.Operation.ADMIN,
            new ResourceAuthorizationAttribute("service", "service2"));
    final Permission permReadUser1 = new Permission("org/zalando/nakadi", AuthorizationService.Operation.READ,
            new ResourceAuthorizationAttribute("user", "user1"));
    final Permission permReadService1 = new Permission("org/zalando/nakadi", AuthorizationService.Operation.READ,
            new ResourceAuthorizationAttribute("service", "service1"));
    final Permission permReadService2 = new Permission("org/zalando/nakadi", AuthorizationService.Operation.READ,
            new ResourceAuthorizationAttribute("service", "service2"));
    final Permission permWriteUser1 = new Permission("org/zalando/nakadi", AuthorizationService.Operation.WRITE,
            new ResourceAuthorizationAttribute("user", "user1"));
    final Permission permWriteService1 = new Permission("org/zalando/nakadi", AuthorizationService.Operation.WRITE,
            new ResourceAuthorizationAttribute("service", "service1"));
    final Permission permWriteService2 = new Permission("org/zalando/nakadi", AuthorizationService.Operation.WRITE,
            new ResourceAuthorizationAttribute("service", "service2"));
    final ResourceAuthorization newAuthz = new ResourceAuthorization(newAttrs, newAttrs, newAttrs);
    final AuthorizationAttribute defaultAdmin = new ResourceAuthorizationAttribute("service", "org/zalando/nakadi");
    private final AuthorizationDbRepository authorizationDbRepository;
    private final AdminService adminService;
    private final AuthorizationService authorizationService;
    private final NakadiSettings nakadiSettings;
    private final List<Permission> adminList;
    private final FeatureToggleService featureToggleService;


    public AdminServiceTest() {
        this.authorizationDbRepository = mock(AuthorizationDbRepository.class);
        this.authorizationService = mock(AuthorizationService.class);
        this.nakadiSettings = mock(NakadiSettings.class);
        this.featureToggleService = mock(FeatureToggleService.class);
        this.adminService = new AdminService(authorizationDbRepository, authorizationService,
                featureToggleService, nakadiSettings);
        this.adminList = new ArrayList<>(Arrays.asList(permAdminUser1, permAdminService1,
                permAdminService2, permReadUser1, permReadService1, permReadService2, permWriteUser1,
                permWriteService1, permWriteService2));
        this.defaultAdminPermissions = new ArrayList<>();
        for (final AuthorizationService.Operation operation : AuthorizationService.Operation.values()) {
            defaultAdminPermissions.add(new Permission("org/zalando/nakadi", operation, defaultAdmin));
        }
        doNothing().when(authorizationService).isAuthorizationForResourceValid(any());
        when(featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS))
                .thenReturn(false);
    }

    @Test
    public void whenUpdateAdminsThenOk() {
        when(authorizationDbRepository.listAdmins()).thenReturn(adminList);
        adminService.updateAdmins(newAuthz.toPermissionsList("org/zalando/nakadi"));
    }

    @Test
    public void whenUpdateThenDefaultAdminIsNotAddedToDB() {
        when(nakadiSettings.getDefaultAdmin()).thenReturn(defaultAdmin);
        when(authorizationDbRepository.listAdmins()).thenReturn(adminList);

        final List<Permission> newList = new ArrayList<>(adminList);
        newList.addAll(defaultAdminPermissions);
        adminService.updateAdmins(newList);
        verify(authorizationDbRepository, times(0)).createPermission(any());
        verify(authorizationDbRepository, times(0)).deletePermission(any());
    }

    @Test
    public void whenUpdateThenDefaultAdminIsNotDeletedFromDB() {
        when(nakadiSettings.getDefaultAdmin()).thenReturn(defaultAdmin);
        when(authorizationDbRepository.listAdmins()).thenReturn(adminList);

        adminService.updateAdmins(adminList);
        verify(authorizationDbRepository, times(0)).createPermission(any());
        verify(authorizationDbRepository, times(0)).deletePermission(any());
    }

    @Test
    public void whenAddNewAdminCallCreatePermission() {
        when(nakadiSettings.getDefaultAdmin()).thenReturn(defaultAdmin);
        when(authorizationDbRepository.listAdmins()).thenReturn(adminList);
        doNothing().when(authorizationDbRepository).update(any(), any());

        final ArgumentCaptor<List> addCaptor = ArgumentCaptor.forClass(List.class);
        final ArgumentCaptor<List> deleteCaptor = ArgumentCaptor.forClass(List.class);

        final List<Permission> newList = new ArrayList<>(adminList);
        newList.add(new Permission("org/zalando/nakadi", AuthorizationService.Operation.READ,
                new ResourceAuthorizationAttribute("user", "user42")));

        adminService.updateAdmins(newList);

        verify(authorizationDbRepository).update(addCaptor.capture(), deleteCaptor.capture());
        assertEquals(1, addCaptor.getValue().size());
        assertEquals(0, deleteCaptor.getValue().size());
    }

    @Test
    public void whenDeleteAdminCallDeletePermission() {
        when(nakadiSettings.getDefaultAdmin()).thenReturn(defaultAdmin);
        when(authorizationDbRepository.listAdmins()).thenReturn(adminList);
        doNothing().when(authorizationDbRepository).update(any(), any());

        final ArgumentCaptor<List> addCaptor = ArgumentCaptor.forClass(List.class);
        final ArgumentCaptor<List> deleteCaptor = ArgumentCaptor.forClass(List.class);

        final List<Permission> newList = new ArrayList<>(adminList);
        newList.remove(permReadUser1);

        adminService.updateAdmins(newList);

        verify(authorizationDbRepository).update(addCaptor.capture(), deleteCaptor.capture());
        assertEquals(0, addCaptor.getValue().size());
        assertEquals(1, deleteCaptor.getValue().size());
    }
}
