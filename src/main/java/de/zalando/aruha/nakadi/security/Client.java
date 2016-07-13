package de.zalando.aruha.nakadi.security;

public interface Client {

    Client PERMIT_ALL = client_id -> true;

    boolean is(String client_id);

    class Authorized implements Client {

        private final String clientId;

        public Authorized(String clientId) {
            this.clientId = clientId;
        }

        @Override
        public boolean is(String clientId) {
            return this.clientId.equals(clientId);
        }
    }

}
