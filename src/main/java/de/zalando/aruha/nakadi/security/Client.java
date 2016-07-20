package de.zalando.aruha.nakadi.security;

public interface Client {

    Client PERMIT_ALL = clientId -> true;

    boolean is(String clientId);

    class Authorized implements Client {

        private final String clientId;

        public Authorized(final String clientId) {
            this.clientId = clientId;
        }

        @Override
        public boolean is(final String clientId) {
            return this.clientId.equals(clientId);
        }
    }

}
