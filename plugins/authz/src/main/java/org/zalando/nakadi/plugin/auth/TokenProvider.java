package org.zalando.nakadi.plugin.auth;

import org.zalando.stups.tokens.AccessTokens;
import org.zalando.stups.tokens.Tokens;

import java.net.URI;
import java.net.URISyntaxException;

public class TokenProvider {

    private final AccessTokens tokens;
    private final String tokenId;

    public TokenProvider(final String tokenPath, final String tokenId) throws URISyntaxException {
        this.tokenId = tokenId;
        final URI accessTokenUri = new URI(tokenPath);
        this.tokens = Tokens.createAccessTokensWithUri(accessTokenUri)
                .manageToken(tokenId)
                .addScope("uid")
                .done()
                .start();
    }

    public String getToken() {
        return tokens.get(tokenId);
    }
}
