package org.zalando.nakadi.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.Subscription;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Temporary class to be used for DB migration of ARUHA-771
 */
public class Aruha771MigrationHelper {

    private HashGenerator hashGenerator;
    private ObjectMapper jsonMapper;
    private final String url;
    private final String user;
    private final String pass;

    public static void main(String[] args) throws Exception {
        final Aruha771MigrationHelper helper = new Aruha771MigrationHelper(
                new HashGenerator(),
                new JsonConfig().jacksonObjectMapper());
        helper.fillSubscriptionsHashes();
    }

    public Aruha771MigrationHelper(final HashGenerator hashGenerator, final ObjectMapper jsonMapper) throws Exception {
        this.hashGenerator = hashGenerator;
        this.jsonMapper = jsonMapper;

        url = "jdbc:postgresql://localhost:5432/local_nakadi_db";
        user = "nakadi";
        pass = "nakadi";
        Class.forName("org.postgresql.Driver").newInstance();
    }

    public void fillSubscriptionsHashes() throws Exception {

        try (Connection con = DriverManager.getConnection(url, user, pass)) {
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery("SELECT s_subscription_object FROM zn_data.subscription");

            int i = 0;
            while (rs.next()) {
                final String jsonSubscription = rs.getString("s_subscription_object");
                final Subscription subscription = jsonMapper.readValue(jsonSubscription, Subscription.class);
                final String hash = hashGenerator.generateSubscriptionKeyFieldsHash(subscription);

                final Statement statement = con.createStatement();
                statement.execute("UPDATE zn_data.subscription SET s_key_fields_hash='" + hash + "' " +
                        "WHERE s_id='" + subscription.getId() + "'");

                System.out.println(++i + " | " + subscription.getId() + " | " + hash);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
