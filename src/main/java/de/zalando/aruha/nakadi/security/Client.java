package de.zalando.aruha.nakadi.security;

public interface Client {

    Client PERMIT_ALL = client_id -> true;

    boolean is(String client_id);

    class Authorized implements Client {

        private final String client_id;

        public Authorized(String client_id) {
            this.client_id = client_id;
        }

        @Override
        public boolean is(String client_id) {
            return this.client_id.equals(client_id);
        }
    }

}
