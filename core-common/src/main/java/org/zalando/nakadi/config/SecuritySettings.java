package org.zalando.nakadi.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
public class SecuritySettings {

    public enum AuthMode {
        OFF,   // no authentication at all
        BASIC, // only checks that the token is valid (has "uid" scope)
        REALM, // checks that the token is valid and contains at least one required realm
        FULL   // full authentication and authorization using oauth2 scopes
    }

    private final String tokenInfoUrl;
    private final String localTokenInfoUrl;
    private final String clientId;
    private final AuthMode authMode;
    public static final String UNAUTHENTICATED_CLIENT_ID = "unauthenticated";

    @Autowired
    public SecuritySettings(@Value("${nakadi.oauth2.tokenInfoUrl}") final String tokenInfoUrl,
                            @Value("${nakadi.oauth2.localTokenInfoUrl}") final String localTokenInfoUrl,
                            @Value("${nakadi.oauth2.clientId}") final String clientId,
                            @Value("${nakadi.oauth2.mode:BASIC}") final AuthMode authMode) {
        this.tokenInfoUrl = tokenInfoUrl;
        this.localTokenInfoUrl = localTokenInfoUrl;
        this.clientId = clientId;
        this.authMode = authMode;
    }

    public String getTokenInfoUrl() {
        return tokenInfoUrl;
    }

    public String getClientId() {
        return clientId;
    }

    public AuthMode getAuthMode() {
        return authMode;
    }

    public String getLocalTokenInfoUrl() {
        return localTokenInfoUrl;
    }
}
