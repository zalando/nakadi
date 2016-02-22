package de.zalando.aruha.nakadi.config;

import org.springframework.beans.factory.annotation.Value;


public class SecuritySettings {

    public enum AuthMode {
        OFF,   // no authentication at all
        BASIC, // only checks that the token is valid (has "uid" scope)
        FULL   // full authentication and authorization using oauth2 scopes
    }

    @Value("${nakadi.oauth2.tokenInfoUri}")
    private String tokenInfoUri;

    @Value("${nakadi.oauth2.clientId}")
    private String clientId;

    @Value("${nakadi.oauth2.mode:BASIC}")
    private AuthMode authMode;

    public String getTokenInfoUri() {
        return tokenInfoUri;
    }

    public String getClientId() {
        return clientId;
    }

    public AuthMode getAuthMode() {
        return authMode;
    }
}
