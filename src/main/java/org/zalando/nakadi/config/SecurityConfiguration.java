package org.zalando.nakadi.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.common.exceptions.OAuth2Exception;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configurers.ResourceServerSecurityConfigurer;
import org.springframework.security.oauth2.provider.error.DefaultOAuth2ExceptionRenderer;
import org.springframework.security.oauth2.provider.error.OAuth2AccessDeniedHandler;
import org.springframework.security.oauth2.provider.error.OAuth2AuthenticationEntryPoint;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.springframework.security.web.firewall.FirewalledRequest;
import org.springframework.security.web.firewall.HttpFirewall;
import org.springframework.security.web.firewall.RequestRejectedException;
import org.springframework.security.web.firewall.StrictHttpFirewall;
import org.zalando.stups.oauth2.spring.security.expression.ExtendedOAuth2WebSecurityExpressionHandler;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    @Value("${nakadi.oauth2.realms}")
    private String realms;

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

    public static String hasUidScopeAndAnyRealm(final String realms) {
        return MessageFormat.format("#oauth2.hasUidScopeAndAnyRealm(''{0}'')", realms);
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
                throws IOException, HttpMessageNotWritableException {
            super.writeInternal(toJsonResponse(object), outputMessage);
        }

        protected Object toJsonResponse(final Object object) {
            if (object instanceof OAuth2Exception) {
                final OAuth2Exception oae = (OAuth2Exception) object;
                if (oae.getCause() != null) {
                    if (oae.getCause() instanceof AuthenticationException) {
                        return new ProblemResponse(Response.Status.UNAUTHORIZED, oae.getCause().getMessage());
                    }
                    return new ProblemResponse(Response.Status.INTERNAL_SERVER_ERROR, oae.getMessage());
                }

                return new ProblemResponse(Response.Status.fromStatusCode(oae.getHttpErrorCode()), oae.getMessage());
            }

            return new ProblemResponse(Response.Status.INTERNAL_SERVER_ERROR,
                    "Unrecognized error happened in authentication path");
        }
    }

    private static class ProblemResponse {
        private final String type;
        private final String title;
        private final int status;
        private final String detail;

        ProblemResponse(final Response.StatusType status, final String detail) {
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

    // TODO: REMOVE IT AFTER EVERYONE HAS NORMALIZED THEIR URLS
    @Bean
    public HttpFirewall allowUrlEncodedSlashHttpFirewall() {
        return new AllowForwardSlashesStrictHttpFirewall();
    }

    // TODO: REMOVE IT AFTER EVERYONE HAS NORMALIZED THEIR URLS
    private static class AllowForwardSlashesStrictHttpFirewall extends StrictHttpFirewall {

        private static final String ENCODED_PERCENT = "%25";
        private static final String PERCENT = "%";
        private static final List<String> FORBIDDEN_ENCODED_PERIOD =
                Collections.unmodifiableList(Arrays.asList("%2e", "%2E"));
        private Set<String> encodedUrlBlacklist = new HashSet<>();
        private Set<String> decodedUrlBlacklist = new HashSet<>();

        AllowForwardSlashesStrictHttpFirewall() {
            super();
            this.encodedUrlBlacklist.add(ENCODED_PERCENT);
            this.encodedUrlBlacklist.addAll(FORBIDDEN_ENCODED_PERIOD);
            this.decodedUrlBlacklist.add(PERCENT);
        }

        private static boolean containsOnlyPrintableAsciiCharacters(final String uri) {
            final int length = uri.length();
            for (int i = 0; i < length; i++) {
                final char c = uri.charAt(i);
                if (c < '\u0020' || c > '\u007e') {
                    return false;
                }
            }

            return true;
        }

        private static boolean encodedUrlContains(final HttpServletRequest request, final String value) {
            if (valueContains(request.getContextPath(), value)) {
                return true;
            }
            return valueContains(request.getRequestURI(), value);
        }

        private static boolean decodedUrlContains(final HttpServletRequest request, final String value) {
            if (valueContains(request.getServletPath(), value)) {
                return true;
            }
            if (valueContains(request.getPathInfo(), value)) {
                return true;
            }
            return false;
        }

        private static boolean valueContains(final String value, final String contains) {
            return value != null && value.contains(contains);
        }

        @Override
        public FirewalledRequest getFirewalledRequest(final HttpServletRequest request)
                throws RequestRejectedException {
            rejectedBlacklistedUrls(request);

            if (!isNormalized(request)) {
                throw new RequestRejectedException("The request was rejected because the URL was not normalized.");
            }

            final String requestUri = request.getRequestURI();
            if (!containsOnlyPrintableAsciiCharacters(requestUri)) {
                throw new RequestRejectedException("The requestURI was rejected because it can only " +
                        "contain printable ASCII characters.");
            }
            return new FirewalledRequest(request) {
                @Override
                public void reset() {
                }
            };
        }

        private static boolean isNormalized(final HttpServletRequest request) {
            if (!isNormalized(request.getRequestURI())) {
                return false;
            }
            if (!isNormalized(request.getContextPath())) {
                return false;
            }
            if (!isNormalized(request.getServletPath())) {
                return false;
            }
            if (!isNormalized(request.getPathInfo())) {
                return false;
            }
            return true;
        }

        private static boolean isNormalized(final String path) {
            if (path == null) {
                return true;
            }

            // ONLY THIS PART IS REMOVED, ALL OTHER CODE IS THE SAME AS IN StrictHttpFirewall
            // if (path.indexOf("//") > -1) {
            //     return false;
            // }

            for (int j = path.length(); j > 0;) {
                final int i = path.lastIndexOf('/', j - 1);
                final int gap = j - i;

                if (gap == 2 && path.charAt(i + 1) == '.') {
                    // ".", "/./" or "/."
                    return false;
                } else if (gap == 3 && path.charAt(i + 1) == '.' && path.charAt(i + 2) == '.') {
                    return false;
                }

                j = i;
            }

            return true;
        }

        private void rejectedBlacklistedUrls(final HttpServletRequest request) {
            for (final String forbidden : this.encodedUrlBlacklist) {
                if (encodedUrlContains(request, forbidden)) {
                    throw new RequestRejectedException("The request was rejected because the URL contained " +
                            "a potentially malicious String \"" + forbidden + "\"");
                }
            }
            for (final String forbidden : this.decodedUrlBlacklist) {
                if (decodedUrlContains(request, forbidden)) {
                    throw new RequestRejectedException("The request was rejected because the URL contained " +
                            "a potentially malicious String \"" + forbidden + "\"");
                }
            }
        }

    }

}
