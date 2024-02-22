package org.zalando.nakadi.plugin.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

public class ZalandoTeamService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZalandoTeamService.class);
    private static final Pattern TEAM_PATTERN = Pattern.compile("^[a-z0-9-]+$");
    private static final String TEAMS_PATH = "api/teams/";
    private static final String OFFICIAL_TEAM_TYPE = "official";
    private final String teamsURL;
    private final HttpClient httpClient;
    private final TokenProvider tokenProvider;
    private final ObjectMapper objectMapper;

    public ZalandoTeamService(
            final String teamsEndpoint,
            final HttpClient httpClient,
            final TokenProvider tokenProvider,
            final ObjectMapper objectMapper) {

        if (teamsEndpoint.endsWith("/")) {
            this.teamsURL = teamsEndpoint + TEAMS_PATH;
        } else {
            this.teamsURL = teamsEndpoint + "/" + TEAMS_PATH;
        }
        this.httpClient = httpClient;
        this.tokenProvider = tokenProvider;
        this.objectMapper = objectMapper;
    }

    public boolean isValidTeam(final String team) throws PluginException {

        if (!TEAM_PATTERN.matcher(team).matches()) {
            return false;
        }

        return fetchTeamInfo(team)
                .map(info -> isTeamInfoValid(team, info))
                .orElse(false);
    }

    public List<String> getTeamMembers(final String team) throws PluginException {
        return fetchTeamInfo(team)
                .map(teamInfo -> Objects.isNull(teamInfo.members)?
                        Collections.<String>emptyList(): Collections.unmodifiableList(teamInfo.members))
                .orElse(Collections.emptyList());
    }

    private boolean isTeamInfoValid(final String team, final TeamInfo teamInfo) {
        if (!OFFICIAL_TEAM_TYPE.equals(teamInfo.type)) {
            return false;
        }
        return team.equals(teamInfo.id) || team.equals(teamInfo.teamId);
    }

    private Optional<TeamInfo> fetchTeamInfo(final String team) {
        final HttpGet request = new HttpGet(teamsURL + team);
        request.addHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
        request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + tokenProvider.getToken());
        try {
            final HttpResponse response = httpClient.execute(request);
            final String responseBody = EntityUtils.toString(response.getEntity());
            final int statusCode = response.getStatusLine().getStatusCode();
            LOGGER.info("Got HTTP {} from {} for team {}; body={}",
                    statusCode, teamsURL, team, responseBody);
            if (statusCode == HttpStatus.SC_OK) {
                final TeamInfo teamInfo = objectMapper.readValue(responseBody, TeamInfo.class);
                return Optional.of(teamInfo);
            } else if (statusCode == HttpStatus.SC_NOT_FOUND) {
                return Optional.empty();
            } else {
                throw new PluginException("Incorrect status code " + statusCode + " when validating " +
                        team + " against " + teamsURL + ". Response body: " + responseBody);
            }
        } catch (final JsonProcessingException jpe) {
            throw new PluginException("Failed to parse response for " + team + " from " + teamsURL, jpe);
        } catch (final IOException ex) {
            throw new PluginException(
                    "Failed to fetch " + team + " from the endpoint " + teamsURL, ex);
        } finally {
            request.releaseConnection();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TeamInfo {
        private final String type;
        private final String id;
        private final String teamId;
        private final List<String> members;

        @JsonCreator
        private TeamInfo(
                @JsonProperty("type") final String type,
                @JsonProperty("id") final String id,
                @JsonProperty("team_id") final String teamId,
                @JsonProperty("member") final List<String> members) {
            this.type = type;
            this.id = id;
            this.teamId = teamId;
            this.members = members;
        }

        @Override
        public String toString() {
            return "TeamInfo{" +
                    "type='" + type + '\'' +
                    ", id='" + id + '\'' +
                    ", teamId='" + teamId + '\'' +
                    ", members=" + members +
                    '}';
        }
    }
}