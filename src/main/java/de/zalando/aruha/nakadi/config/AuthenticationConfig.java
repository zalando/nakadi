package de.zalando.aruha.nakadi.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.zalando.stups.oauth2.spring.server.TokenInfoResourceServerTokenServices;

@Configuration
@Profile("!test")
public class AuthenticationConfig {

    @Bean
    public ResourceServerTokenServices zalandoResourceTokenServices() {
        return new TokenInfoResourceServerTokenServices(securitySettings().getTokenInfoUri(), securitySettings().getClientId());
    }

    @Bean
    public SecuritySettings securitySettings() {
        return new SecuritySettings();
    }
}
