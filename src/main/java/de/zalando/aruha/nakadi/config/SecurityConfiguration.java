package de.zalando.aruha.nakadi.config;

import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configurers.ResourceServerSecurityConfigurer;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;

import org.zalando.stups.oauth2.spring.server.TokenInfoResourceServerTokenServices;

import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpMethod.PUT;

@EnableResourceServer
@Configuration
public class SecurityConfiguration extends ResourceServerConfigurerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityConfiguration.class);

    public static final String UID = "uid";
    public static final String NAKADI_READ_SCOPE = "nakadi.read";
    public static final String EVENT_TYPE_WRITE_SCOPE = "nakadi_event_type.write";
    public static final String EVENT_STREAM_READ_SCOPE = "nakadi_event_stream.read";
    public static final String EVENT_STREAM_WRITE_SCOPE = "nakadi_event_stream.write";

    public enum AuthMode {
        OFF,
        BASIC,
        FULL
    }

    @Value("${nakadi.oauth2.tokenInfoUri}")
    private String tokenInfoUri;

    @Value("${nakadi.oauth2.clientId}")
    private String clientId;

    @Value("${nakadi.oauth2.mode:BASIC}")
    private AuthMode authMode;

    @Override
    public void configure(final HttpSecurity http) throws Exception {
        LOG.info("Authentication mode: " + authMode);
        if (authMode == AuthMode.FULL) {
            http.authorizeRequests()
                    .antMatchers("/health").permitAll()
                    .antMatchers(GET, "/metrics").access(hasScope(NAKADI_READ_SCOPE))
                    .antMatchers(GET, "/event-types").access(hasScope(NAKADI_READ_SCOPE))
                    .antMatchers(GET, "/event-types/*").access(hasScope(NAKADI_READ_SCOPE))
                    .antMatchers(POST, "/event-types").access(hasScope(EVENT_TYPE_WRITE_SCOPE))
                    .antMatchers(PUT, "/event-types/*").access(hasScope(EVENT_TYPE_WRITE_SCOPE))
                    .antMatchers(GET, "/event-types/*/events").access(hasScope(EVENT_STREAM_READ_SCOPE))
                    .antMatchers(POST, "/event-types/*/events").access(hasScope(EVENT_STREAM_WRITE_SCOPE))
                    .antMatchers(GET, "/event-types/*/partitions").access(hasScope(EVENT_STREAM_READ_SCOPE))
                    .antMatchers(GET, "/event-types/*/partitions/*").access(hasScope(EVENT_STREAM_READ_SCOPE))
                    .anyRequest().access(hasScope(UID));
        }
        else if (authMode == AuthMode.BASIC) {
            http.authorizeRequests()
                    .antMatchers("/health").permitAll()
                    .anyRequest().access(hasScope(UID));
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
        resources.tokenServices(zalandoResourceTokenServices());
    }

    @Bean
    public ResourceServerTokenServices zalandoResourceTokenServices() {
        return new TokenInfoResourceServerTokenServices(tokenInfoUri, clientId);
    }
}
