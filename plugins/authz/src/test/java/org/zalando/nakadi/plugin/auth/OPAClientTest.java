package org.zalando.nakadi.plugin.auth;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.net.SocketTimeoutException;
import java.util.Set;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OPAClientTest {

    private static final String MOCK_TOKEN = UUID.randomUUID().toString();

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089);

    private TokenProvider tokenProvider = mock(TokenProvider.class);

    private OPAClient opaClient;

    @Before
    public void init() {
        when(tokenProvider.getToken()).thenReturn(MOCK_TOKEN);
        this.opaClient = new FilterConfig().opaClient(
                wireMockRule.baseUrl(), "/v1/test/policy", tokenProvider,
                10, 50, 100, 100, 3, OpaDegradationPolicy.THROW);
    }

    @Test
    public void getRetailerIdsForService() {
        final String appId = UUID.randomUUID().toString();

        stubOPA(new JSONObject().put("service", new JSONObject().put("kio_app_id", appId)),
                new JSONArray().put("zalando"));

        final Set<String> retailerIdsForService = opaClient.getRetailerIdsForService(appId);

        assertEquals(1, retailerIdsForService.size());
        assertTrue(retailerIdsForService.contains("zalando"));

    }

    @Test
    public void getRetailerIdsForUser() {

        final String userId = UUID.randomUUID().toString();

        stubOPA(new JSONObject().put("user", new JSONObject().put("ldap_name", userId)),
                new JSONArray().put("zalando-lounge"));

        final Set<String> retailerIdsForUser = opaClient.getRetailerIdsForUser(userId);

        assertEquals(1, retailerIdsForUser.size());
        assertTrue(retailerIdsForUser.contains("zalando-lounge"));
    }

    @Test
    public void retryAndTimeout() {

        final String userId = UUID.randomUUID().toString();

        stubFor(post("/v1/test/policy").willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(new JSONObject()
                        .put("result", new JSONObject()
                                .put("decision", new JSONObject()
                                        .put("retailer_ids", new JSONArray().put("zalando")))).toString())
                .withFixedDelay(220)));

        try {
            final Set<String> retailerIdsForUser = opaClient.getRetailerIdsForUser(userId);
            assertEquals(1, retailerIdsForUser.size());
            assertTrue(retailerIdsForUser.contains("zalando"));
        } catch (final PluginException pe) {
            assertTrue(pe.getCause() instanceof RuntimeException);
            assertTrue(pe.getCause().getCause() instanceof SocketTimeoutException);
        }
        verify(3, postRequestedFor(urlEqualTo("/v1/test/policy")));
    }

    @Test
    public void handleMalformedResponse() {

        final String userId = UUID.randomUUID().toString();

        final OPAClient client = new FilterConfig().opaClient(
                wireMockRule.baseUrl(), "/v1/test/policy", tokenProvider,
                10, 50, 100, 100, 3, OpaDegradationPolicy.PERMIT);

        stubFor(post("/v1/test/policy").willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{}")
                .withFixedDelay(20)));

        final Set<String> retailerIdsForUser = client.getRetailerIdsForUser(userId);
        assertEquals(Set.of("*"), retailerIdsForUser);

        verify(1, postRequestedFor(urlEqualTo("/v1/test/policy")));
    }

    private void stubOPA(final JSONObject input, final JSONArray result) {
        stubFor(post(urlEqualTo("/v1/test/policy"))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .withRequestBody(equalToJson(new JSONObject().put("input", input).toString()))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(new JSONObject()
                                .put("result", new JSONObject()
                                        .put("decision", new JSONObject()
                                                .put("retailer_ids", result))).toString())));

    }
}
