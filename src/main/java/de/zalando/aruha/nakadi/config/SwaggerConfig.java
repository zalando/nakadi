package de.zalando.aruha.nakadi.config;

import static com.google.common.base.Predicates.or;

import static springfox.documentation.builders.PathSelectors.regex;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.base.Predicate;

import springfox.documentation.builders.ApiInfoBuilder;

import springfox.documentation.service.ApiInfo;

import springfox.documentation.spi.DocumentationType;

import springfox.documentation.spring.web.plugins.Docket;

import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
@EnableAutoConfiguration
public class SwaggerConfig {

    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2).groupName("api").apiInfo(apiInfo()).select().paths(apiPaths())
                                                      .build();
    }

    private Predicate<String> apiPaths() {
        return or(regex("/api/echo.*"));
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder().title("Bazaar prototype")
                                   .description("Prototype for next gen technologies for Zalando - Bazaar team.")
                                   .contact("springfox").license("Apache License Version 2.0")
                                   .licenseUrl("https://github.com/springfox/springfox/blob/master/LICENSE")
                                   .version("2.0").build();
    }
}
