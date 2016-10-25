package org.zalando.nakadi.security;

public class Client {

    private final String id;
    private final Permissions permissions;

    public Client(final String id, final Permissions permissions) {
        this.id = id;
        this.permissions = permissions;
    }

    public String getId() {
        return id;
    }

    public Permissions getPermissions() {
        return permissions;
    }
}
