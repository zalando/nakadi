package org.zalando.nakadi.plugin.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.zalando.nakadi.plugin.auth.utils.Fixture;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ZalandoTeamServiceTest {
    private static final String MOCK_TOKEN = UUID.randomUUID().toString();

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8090);
    private final TokenProvider tokenProvider = mock(TokenProvider.class);

    private ZalandoTeamService teamService;

    @Before
    public void init() {
        when(tokenProvider.getToken()).thenReturn(MOCK_TOKEN);
        this.teamService = new ZalandoTeamService(
                wireMockRule.baseUrl(),
                HttpClientBuilder.create()
                        .setUserAgent("nakadi")
                        .evictIdleConnections(1L, TimeUnit.MINUTES)
                        .evictExpiredConnections()
                        .build(),
                tokenProvider,
                new ObjectMapper());
    }

    @Test
    public void isValidTeamAuthorizationAttribute() throws IOException {

        WireMock.stubFor(get(urlEqualTo("/api/teams/team-not-found"))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse()
                        .withStatus(404)
                        .withBody("Not found")));
        Assert.assertFalse("Should return false for non-existent team",
                teamService.isValidTeam("team-not-found"));

        WireMock.stubFor(get(urlEqualTo("/api/teams/stups"))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(Fixture.fixture("/fixtures/stups-team-response.json"))));
        Assert.assertFalse("Should return false for virtual team",
                teamService.isValidTeam("stups"));

        WireMock.stubFor(get(urlEqualTo("/api/teams/aruha"))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(Fixture.fixture("/fixtures/aruha-team-response.json"))));
        Assert.assertTrue("Should return true for official team",
                teamService.isValidTeam("aruha"));
    }

    @Test
    public void getMembers() throws IOException {
        WireMock.stubFor(get(urlEqualTo("/api/teams/aruha"))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(Fixture.fixture("/fixtures/aruha-team-response.json"))));
        final var members = teamService.getTeamMembers("aruha");
        assertEquals("Should return 6 members for aruha", 6, members.size());
    }

    @Test
    public void getEmptyListWhenZeroMember() throws IOException {
        final var objectMapper = new ObjectMapper();
        final var teamInfo = objectMapper.readTree(Fixture.fixture("/fixtures/aruha-team-response.json"));
        ((ObjectNode)teamInfo).putNull("member");

        WireMock.stubFor(get(urlEqualTo("/api/teams/aruha"))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(teamInfo.toString())));
        final var members = teamService.getTeamMembers("aruha");
        assertEquals("Should return 0 members for aruha", 0, members.size());
    }

}
