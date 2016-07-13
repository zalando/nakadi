package de.zalando.aruha.nakadi.security;

public interface Client {

    Client PERMIT_ALL = clientId -> true;

    boolean is(String clientId);

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
