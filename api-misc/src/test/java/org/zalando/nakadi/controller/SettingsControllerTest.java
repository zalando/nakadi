package org.zalando.nakadi.controller;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.utils.TestUtils;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.zalando.nakadi.config.SecuritySettings.AuthMode.OFF;
import static org.zalando.nakadi.service.BlacklistService.Type.CONSUMER_ET;
import static org.zalando.nakadi.util.PrincipalMockFactory.mockPrincipal;


public class SettingsControllerTest {

    private final BlacklistService blacklistService = mock(BlacklistService.class);
    private final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
    private final AdminService adminService = mock(AdminService.class);

    private final SecuritySettings securitySettings = Mockito.mock(SecuritySettings.class);
    private final AuthorizationService authorizationService = mock(AuthorizationService.class);
    private MockMvc mockMvc;

    @Before
    public void before() {
        final SettingsController controller = new SettingsController(
                blacklistService, featureToggleService, adminService);

        when(securitySettings.getAuthMode()).thenReturn(OFF);
        when(securitySettings.getAdminClientId()).thenReturn("org/zalando/nakadi");
        when(authorizationService.getSubject()).thenReturn(Optional.empty());
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setCustomArgumentResolvers(new ClientResolver(
                        securitySettings, authorizationService))
                .setControllerAdvice(new NakadiProblemExceptionHandler())
                .build();
    }

    @Test
    public void testBlocking() throws Exception {
        when(adminService.isAdmin(any())).thenReturn(true);
        mockMvc.perform(put("/settings/blacklist/CONSUMER_ET/RmTuJqguXSlwisivWQB.tpZCvBJrbrwphnzQcUwkrClVIjrjJm")
                .contentType(MediaType.APPLICATION_JSON)
                .principal(mockPrincipal("org/zalando/nakadi")))
                .andExpect(status().isNoContent());

        verify(blacklistService).blacklist("RmTuJqguXSlwisivWQB.tpZCvBJrbrwphnzQcUwkrClVIjrjJm", CONSUMER_ET);
    }

}
