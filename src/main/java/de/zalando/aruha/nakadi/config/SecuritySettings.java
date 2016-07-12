package de.zalando.aruha.nakadi.config;

import org.springframework.beans.factory.annotation.Value;


public class SecuritySettings {

    public enum AuthMode {
        OFF,   // no authentication at all
        BASIC, // only checks that the token is valid (has "uid" scope)
        FULL   // full authentication and authorization using oauth2 scopes
    }

    @Value("${nakadi.oauth2.tokenInfoUrl}")
    private String tokenInfoUrl;

    @Value("${nakadi.oauth2.clientId}")
    private String clientId;

    @Value("${nakadi.oauth2.mode:BASIC}")
    private AuthMode authMode;

    @Value("${nakadi.oauth2.defaultClientId}")
    private String defaultClientId;

    public String getTokenInfoUrl() {
        return tokenInfoUrl;
    }

    public String getClientId() {
        return clientId;
    }

    public AuthMode getAuthMode() {
        return authMode;
    }

    public String getDefaultClientId() {
        return defaultClientId;
    }
}
