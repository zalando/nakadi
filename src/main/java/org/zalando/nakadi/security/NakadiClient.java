package org.zalando.nakadi.security;

public class NakadiClient extends Client {

    private final String realm;

    public NakadiClient(final String clientId, final String realm) {
        super(clientId);
        this.realm = realm;
    }

    @Override
    public String getRealm() {
        return realm;
    }
}
