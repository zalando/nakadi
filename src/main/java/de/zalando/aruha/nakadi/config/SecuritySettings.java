package de.zalando.aruha.nakadi.config;

import org.springframework.beans.factory.annotation.Value;


public class SecuritySettings {

    public enum AuthMode {
        OFF,
        BASIC,
        FULL
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
