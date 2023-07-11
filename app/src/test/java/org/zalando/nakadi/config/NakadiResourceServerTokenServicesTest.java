package org.zalando.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.security.oauth2.common.exceptions.OAuth2Exception;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.stups.oauth2.spring.server.TokenInfoResourceServerTokenServices;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NakadiResourceServerTokenServicesTest {
    @Mock
    private MetricRegistry metricRegistry;
    @Mock
    private TokenInfoResourceServerTokenServices localService;
    @Mock
    private TokenInfoResourceServerTokenServices remoteService;
    @Mock
    private FeatureToggleService featureToggleService;
    private NakadiResourceServerTokenServices objectToTest;

    @Before
    public void prepare() {
        final Timer timer = mock(Timer.class);
        final Timer.Context context = mock(Timer.Context.class);
        when(timer.time()).thenReturn(context);
        when(metricRegistry.timer(any())).thenReturn(timer);

        objectToTest = new NakadiResourceServerTokenServices(
                metricRegistry, localService, remoteService, featureToggleService);
    }

    @Test
    public void whenLocalIisDisabledItIsNotUsed() {
        when(featureToggleService.isFeatureEnabled(eq(Feature.REMOTE_TOKENINFO))).thenReturn(true);
        when(remoteService.loadAuthentication(any())).thenReturn(mock(OAuth2Authentication.class));

        objectToTest.loadAuthentication("bbb");

        verify(localService, times(0)).loadAuthentication(any());
        verify(remoteService, times(1)).loadAuthentication(eq("bbb"));
    }

    @Test
    public void whenLocalHasValidResponseRemoteIsNotCalled() {
        when(featureToggleService.isFeatureEnabled(eq(Feature.REMOTE_TOKENINFO))).thenReturn(false);

        final OAuth2Authentication expectedResponse = mock(OAuth2Authentication.class);
        when(localService.loadAuthentication(any())).thenReturn(expectedResponse);

        final OAuth2Authentication response = objectToTest.loadAuthentication("bbb");

        verifyNoInteractions(remoteService);
        assertSame(expectedResponse, response);
    }

    @Test
    public void whenLocalHasBadResponseRemoteIsNotCalled() {
        when(featureToggleService.isFeatureEnabled(eq(Feature.REMOTE_TOKENINFO))).thenReturn(false);

        final OAuth2Exception expectedException = mock(OAuth2Exception.class);
        when(localService.loadAuthentication(any())).thenThrow(expectedException);

        try {
            objectToTest.loadAuthentication("bbb");
            fail();
        } catch (OAuth2Exception ex) {
            assertSame(expectedException, ex);
        }
        verifyNoInteractions(remoteService);
    }

    @Test
    public void whenLocalBrokenRemoteValidUsed() {
        when(featureToggleService.isFeatureEnabled(eq(Feature.REMOTE_TOKENINFO))).thenReturn(false);

        when(localService.loadAuthentication(any())).thenThrow(mock(RuntimeException.class));
        final OAuth2Authentication expectedResponse = mock(OAuth2Authentication.class);
        when(remoteService.loadAuthentication(any())).thenReturn(expectedResponse);

        final OAuth2Authentication response = objectToTest.loadAuthentication("bbb");
        assertSame(expectedResponse, response);
    }

    @Test
    public void whenLocalBrokenRemoteBadUsed() {
        when(featureToggleService.isFeatureEnabled(eq(Feature.REMOTE_TOKENINFO))).thenReturn(false);

        when(localService.loadAuthentication(any())).thenThrow(mock(RuntimeException.class));
        final OAuth2Exception expectedException = mock(OAuth2Exception.class);
        when(remoteService.loadAuthentication(any())).thenThrow(expectedException);

        try{
            objectToTest.loadAuthentication("bbb");
            fail();
        } catch (OAuth2Exception ex) {
            assertSame(expectedException, ex);
        }
    }

    @Test
    public void whenLocalIsUnavailableRemoteIsUsed() {
        when(featureToggleService.isFeatureEnabled(eq(Feature.REMOTE_TOKENINFO))).thenReturn(false);
        when(localService.loadAuthentication(any())).thenThrow(new ServiceTemporarilyUnavailableException(""));
        objectToTest.loadAuthentication("bbb");
        verify(localService, times(1)).loadAuthentication(eq("bbb"));
        verify(remoteService, times(1)).loadAuthentication(eq("bbb"));
    }

    @Test
    public void whenLocalBrokenRemote500Replaced() {
        when(featureToggleService.isFeatureEnabled(eq(Feature.REMOTE_TOKENINFO))).thenReturn(false);

        when(localService.loadAuthentication(any())).thenThrow(mock(RuntimeException.class));
        when(remoteService.loadAuthentication(any())).thenThrow(new RuntimeException("msg"));

        try{
            objectToTest.loadAuthentication("bbb");
            fail();
        } catch (OAuth2Exception ex) {
            assertEquals("msg", ex.getMessage());
            assertEquals(HttpStatus.SERVICE_UNAVAILABLE.value(), ex.getHttpErrorCode());
        }
    }
}
