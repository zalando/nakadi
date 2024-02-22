package org.zalando.nakadi.plugin.auth;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class ServiceFactoryTest {

    private final HttpClient httpClient;

    private final TokenProvider tokenProvider;

    private final ValueRegistry valueRegistryToCheck;

    public ServiceFactoryTest(
            final HttpClient httpClient,
            final TokenProvider tokenProvider,
            final ValueRegistry valueRegistryToCheck,
            final String ignore) {
        this.httpClient = httpClient;
        this.tokenProvider = tokenProvider;
        this.valueRegistryToCheck = valueRegistryToCheck;
    }

    @Parameterized.Parameters(name = "{index}: {3}")
    public static Iterable<Object[]> getParameters() {
        final List<Object[]> result = new ArrayList<>();
        final HttpClient httpClient = mock(HttpClient.class);
        final TokenProvider tokenProvider = mock(TokenProvider.class);
        final Object[] v = new Object[]{
                httpClient,
                tokenProvider,
                ServiceFactory.createUserRegistryInternal("user_url", httpClient, tokenProvider),
                "user"
        };
        result.add(v);
        result.add(new Object[]{
                httpClient,
                tokenProvider,
                ServiceFactory.createMerchantRegistryInternal("merchant_url", httpClient, tokenProvider),
                "merchant"
        });
        result.add(new Object[]{
                httpClient,
                tokenProvider,
                ServiceFactory.createApplicationRegistryInternal("user_url", httpClient, tokenProvider),
                "application"
        });
        return result;
    }

    @Before
    public void resetMocks() {
        Mockito.reset(tokenProvider, httpClient);
    }

    @Test
    public void verifyEmptyValueIsNotValidAndNotRequestedThroughHttp() {
        assertFalse(valueRegistryToCheck.isValid(""));
        Mockito.verifyNoInteractions(httpClient);
    }

    @Test
    public void verifyRedirectsAreNotConsidered() {
        assertFalse(valueRegistryToCheck.isValid("../."));
        Mockito.verifyNoInteractions(httpClient);
    }

    @Test
    public void verifyNonEncodableData() {
        assertFalse(valueRegistryToCheck.isValid("="));
        Mockito.verifyNoInteractions(httpClient);
    }

    @Test
    public void verifyValueFoundBehavior() throws IOException {
        final ArgumentCaptor<HttpUriRequest> request = mockHttpResponse(HttpStatus.SC_OK);
        assertTrue(valueRegistryToCheck.isValid("found-value"));

        final HttpUriRequest value = request.getValue();
        assertTrue(value.getURI().getPath().endsWith("/found-value"));
    }

    @Test
    public void verifyValueNotFoundBehavior() throws IOException {
        mockHttpResponse(HttpStatus.SC_NOT_FOUND);
        assertFalse(valueRegistryToCheck.isValid("notfound-value"));
    }

    @Test
    public void verifyUnexpectedStatusCode() throws IOException {
        mockHttpResponse(HttpStatus.SC_MULTI_STATUS);
        assertThrows(PluginException.class,
                () -> valueRegistryToCheck.isValid("unexpected-value"));
    }

    private ArgumentCaptor<HttpUriRequest> mockHttpResponse(final int status) throws IOException {
        final HttpResponse resp = mock(HttpResponse.class);
        final HttpEntity entity = mock(HttpEntity.class);

        when(resp.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, status, "OK"));

        final ArgumentCaptor<HttpUriRequest> captor = ArgumentCaptor.forClass(HttpUriRequest.class);
        when(httpClient.execute(captor.capture())).thenReturn(resp);
        when(resp.getEntity()).thenReturn(entity);
        return captor;
    }
}