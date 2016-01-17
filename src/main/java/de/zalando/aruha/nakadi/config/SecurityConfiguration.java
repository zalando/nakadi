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

@EnableResourceServer
@Configuration
public class SecurityConfiguration extends ResourceServerConfigurerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(SecurityConfiguration.class);
  public static final String UID = "uid";

  @Value("${nakadi.oauth2.tokenInfoUri}")
  private String tokenInfoUri;

  @Value("${nakadi.oauth2.clientId}")
  private String clientId;

  @Value("${nakadi.oauth2.enabled:true}")
  private boolean oauthEnabled;

  @Override
  public void configure(final HttpSecurity http) throws Exception {
    LOG.info("Security: OAuth2 is " + (oauthEnabled ? "enabled" : "disabled"));
    if (oauthEnabled) {
      http.authorizeRequests()
          .antMatchers("/health")
          .permitAll()
          .anyRequest()
          .access(hasScope(UID));
    } else {
      http.authorizeRequests().anyRequest().permitAll();
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
