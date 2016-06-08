package de.zalando.aruha.nakadi.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configurers.ResourceServerSecurityConfigurer;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;

import java.text.MessageFormat;

import static org.springframework.http.HttpMethod.DELETE;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpMethod.PUT;

@EnableResourceServer
@Configuration
public class SecurityConfiguration extends ResourceServerConfigurerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityConfiguration.class);

    @Autowired
    private SecuritySettings settings;

    @Autowired
    private ResourceServerTokenServices tokenServices;

    @Value("${nakadi.oauth2.scopes.uid}")
    private String uidScope;

    @Value("${nakadi.oauth2.scopes.nakadiAdmin}")
    private String nakadiAdminScope;

    @Value("${nakadi.oauth2.scopes.eventTypeWrite}")
    private String eventTypeWriteScope;

    @Value("${nakadi.oauth2.scopes.eventStreamRead}")
    private String eventStreamReadScope;

    @Value("${nakadi.oauth2.scopes.eventStreamWrite}")
    private String eventStreamWriteScope;

    @Override
    public void configure(final HttpSecurity http) throws Exception {
        LOG.info("Authentication mode: " + settings.getAuthMode());

        if (settings.getAuthMode() == SecuritySettings.AuthMode.FULL) {
            http.authorizeRequests()
                    .antMatchers(GET, "/event-types/*/partitions/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(GET, "/event-types/*/events/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(GET, "/subscriptions/*/events/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(POST, "/event-types/*/events/**").access(hasScope(eventStreamWriteScope))
                    .antMatchers(DELETE, "/event-types/*/**").access(hasScope(nakadiAdminScope))
                    .antMatchers(POST, "/event-types/**").access(hasScope(eventTypeWriteScope))
                    .antMatchers(PUT, "/event-types/**").access(hasScope(eventTypeWriteScope))
                    .antMatchers(GET, "/health/**").permitAll()
                    .anyRequest().access(hasScope(uidScope));
        }
        else if (settings.getAuthMode() == SecuritySettings.AuthMode.BASIC) {
            http.authorizeRequests()
                    .antMatchers(GET, "/health/**").permitAll()
                    .anyRequest().access(hasScope(uidScope));
        }
        else {
            http.authorizeRequests()
                    .anyRequest().permitAll();
        }
    }

    public static String hasScope(final String scope) {
        return MessageFormat.format("#oauth2.hasScope(''{0}'')", scope);
    }

    @Override
    public void configure(final ResourceServerSecurityConfigurer resources) throws Exception {
        resources.tokenServices(tokenServices);
    }

}
