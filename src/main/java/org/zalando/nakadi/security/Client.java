package org.zalando.nakadi.security;

public abstract class Client {

    private final String clientId;

    public Client(final String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }

    public String getRealm() {
        return "";
    }
}
