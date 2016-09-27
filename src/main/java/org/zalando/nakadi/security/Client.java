package org.zalando.nakadi.security;

public class Client {

    private final String id;
    private final Permissions permissions;

    public Client(String id, Permissions permissions) {
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
