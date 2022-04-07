package org.zalando.nakadi.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.common.exceptions.OAuth2Exception;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configurers.ResourceServerSecurityConfigurer;
import org.springframework.security.oauth2.provider.error.DefaultOAuth2ExceptionRenderer;
import org.springframework.security.oauth2.provider.error.OAuth2AccessDeniedHandler;
import org.springframework.security.oauth2.provider.error.OAuth2AuthenticationEntryPoint;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.zalando.nakadi.exceptions.runtime.UnknownStatusCodeException;
import org.zalando.problem.Status;
import org.zalando.problem.StatusType;
import org.zalando.stups.oauth2.spring.security.expression.ExtendedOAuth2WebSecurityExpressionHandler;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.http.HttpMethod.DELETE;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpMethod.PUT;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.UNAUTHORIZED;

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

    @Value("${nakadi.oauth2.realms}")
    private String[] realms;

    @Value("${nakadi.oauth2.scopes.nakadiAdmin}")
    private String nakadiAdminScope;

    @Value("${nakadi.oauth2.scopes.eventTypeWrite}")
    private String eventTypeWriteScope;

    @Value("${nakadi.oauth2.scopes.eventStreamRead}")
    private String eventStreamReadScope;

    @Value("${nakadi.oauth2.scopes.eventStreamWrite}")
    private String eventStreamWriteScope;

    public static String hasScope(final String scope) {
        return MessageFormat.format("#oauth2.hasScope(''{0}'')", scope);
    }

    public static String hasUidScopeAndAnyRealm(final String[] realms) {
        final String formattedRealms = Arrays.stream(realms)
                .map(r -> MessageFormat.format("''{0}''", r))
                .collect(Collectors.joining(","));
        return MessageFormat.format("#oauth2.hasUidScopeAndAnyRealm({0})", formattedRealms);
    }

    @Override
    public void configure(final HttpSecurity http) throws Exception {
        LOG.info("Authentication mode: " + settings.getAuthMode());

        if (settings.getAuthMode() == SecuritySettings.AuthMode.FULL) {
            http.authorizeRequests()
                    .antMatchers(GET, "/event-types/*/partitions/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(GET, "/event-types/*/events/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(GET, "/event-types/*/cursor-distances/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(GET, "/event-types/*/shifted-cursors/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(GET, "/event-types/*/cursors-lag/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(GET, "/subscriptions/*/events/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(GET, "/subscriptions/*/cursors/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(POST, "/subscriptions/*/cursors/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(GET, "/subscriptions/*/stats/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(POST, "/event-types/*/shifted-cursors").access(hasScope(eventStreamReadScope))
                    .antMatchers(POST, "/event-types/*/cursors-lag").access(hasScope(eventStreamReadScope))
                    .antMatchers(POST, "/event-types/*/cursor-distances").access(hasScope(eventStreamReadScope))
                    .antMatchers(POST, "/event-types/*/events/**").access(hasScope(eventStreamWriteScope))
                    .antMatchers(DELETE, "/event-types/*/**").access(hasScope(nakadiAdminScope))
                    .antMatchers(POST, "/event-types/**").access(hasScope(eventTypeWriteScope))
                    .antMatchers(PUT, "/event-types/**").access(hasScope(eventTypeWriteScope))
                    .antMatchers(GET, "/subscriptions/*/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(DELETE, "/subscriptions/*/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(POST, "/subscriptions/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(GET, "/subscriptions/**").access(hasScope(eventStreamReadScope))
                    .antMatchers(GET, "/health/**").permitAll()
                    .anyRequest().access(hasScope(uidScope));
        } else if (settings.getAuthMode() == SecuritySettings.AuthMode.BASIC) {
            http.authorizeRequests()
                    .antMatchers(GET, "/health/**").permitAll()
                    .anyRequest().access(hasScope(uidScope));
        } else if (settings.getAuthMode() == SecuritySettings.AuthMode.REALM) {
            http.authorizeRequests()
                    .antMatchers(GET, "/health/**").permitAll()
                    .anyRequest().access(hasUidScopeAndAnyRealm(realms));
        } else {
            http.authorizeRequests()
                    .anyRequest().permitAll();
        }
    }

    @Override
    public void configure(final ResourceServerSecurityConfigurer resources) throws Exception {
        final OAuth2AuthenticationEntryPoint oAuth2AuthenticationEntryPoint = new OAuth2AuthenticationEntryPoint();
        oAuth2AuthenticationEntryPoint.setExceptionRenderer(new ProblemOauthExceptionRenderer());
        resources.authenticationEntryPoint(oAuth2AuthenticationEntryPoint);
        resources.tokenServices(tokenServices);
        resources.expressionHandler(new ExtendedOAuth2WebSecurityExpressionHandler());
        final OAuth2AccessDeniedHandler oAuth2AccessDeniedHandler = new OAuth2AccessDeniedHandler();
        oAuth2AccessDeniedHandler.setExceptionRenderer(new ProblemOauthExceptionRenderer());
        resources.accessDeniedHandler(oAuth2AccessDeniedHandler);
    }

    private static class ProblemOauthExceptionRenderer extends DefaultOAuth2ExceptionRenderer {

        ProblemOauthExceptionRenderer() {
            final List<HttpMessageConverter<?>> messageConverters = new ArrayList<>();
            messageConverters.add(new ProblemOauthMessageConverter());
            setMessageConverters(messageConverters);
        }
    }

    private static class ProblemOauthMessageConverter extends MappingJackson2HttpMessageConverter {

        @Override
        protected void writeInternal(final Object object, final HttpOutputMessage outputMessage)
                throws IOException, HttpMessageNotWritableException, UnknownStatusCodeException {
            super.writeInternal(toJsonResponse(object), outputMessage);
        }

        protected Object toJsonResponse(final Object object) throws UnknownStatusCodeException {
            if (object instanceof OAuth2Exception) {
                final OAuth2Exception oae = (OAuth2Exception) object;
                if (oae.getCause() != null) {
                    if (oae.getCause() instanceof AuthenticationException) {
                        return new ProblemResponse(UNAUTHORIZED, oae.getCause().getMessage());
                    }
                    return new ProblemResponse(INTERNAL_SERVER_ERROR, oae.getMessage());
                }

                return new ProblemResponse(fromStatusCode(oae.getHttpErrorCode()), oae.getMessage());
            }

            return new ProblemResponse(INTERNAL_SERVER_ERROR,
                    "Unrecognized error happened in authentication path");
        }
    }

    private static class ProblemResponse {
        private final String type;
        private final String title;
        private final int status;
        private final String detail;

        ProblemResponse(final StatusType status, final String detail) {
            this.type = "https://httpstatus.es/" + status.getStatusCode();
            this.title = status.getReasonPhrase();
            this.status = status.getStatusCode();
            this.detail = detail;
        }

        public String getType() {
            return type;
        }

        public String getTitle() {
            return title;
        }

        public int getStatus() {
            return status;
        }

        public String getDetail() {
            return detail;
        }
    }

    private static Status fromStatusCode(final int code) throws UnknownStatusCodeException {
        for (final Status status: Status.values()) {
            if (status.getStatusCode() == code) {
                return status;
            }
        }
        throw new UnknownStatusCodeException("Unknown status code: " + code);
    }

    @Configuration
    @EnableWebSecurity
    public class WebSecurityConfiguration extends WebSecurityConfigurerAdapter {
        @Override
        public void configure(final WebSecurity web) throws Exception {
            if (settings.getAuthMode() == SecuritySettings.AuthMode.OFF) {
                web.ignoring().anyRequest();
            }
        }
    }

}
