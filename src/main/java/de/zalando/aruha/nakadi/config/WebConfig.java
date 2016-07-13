package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.metrics.MonitoringRequestFilter;
import de.zalando.aruha.nakadi.security.ClientResolver;
import de.zalando.aruha.nakadi.util.FeatureToggleService;
import de.zalando.aruha.nakadi.util.FlowIdRequestFilter;
import de.zalando.aruha.nakadi.util.GzipBodyRequestFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.embedded.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.xml.SourceHttpMessageConverter;
import org.springframework.web.context.request.async.TimeoutCallableProcessingInterceptor;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import javax.servlet.Filter;
import java.util.List;

import static de.zalando.aruha.nakadi.config.NakadiConfig.METRIC_REGISTRY;

@Configuration
public class WebConfig extends WebMvcConfigurationSupport {

    @Value("${nakadi.stream.timeoutMs}")
    private long nakadiStreamTimeout;

    @Autowired
    private JsonConfig jsonConfig;

    @Autowired
    private SecuritySettings securitySettings;

    @Autowired
    private FeatureToggleService featureToggleService;

    @Override
    public void configureAsyncSupport(final AsyncSupportConfigurer configurer) {
        configurer.setDefaultTimeout(nakadiStreamTimeout);
        configurer.registerCallableInterceptors(timeoutInterceptor());
    }

    @Bean
    public TimeoutCallableProcessingInterceptor timeoutInterceptor() {
        return new TimeoutCallableProcessingInterceptor();
    }

    @Bean
    public FilterRegistrationBean flowIdRequestFilter() {
        return createFilterRegistrationBean(new FlowIdRequestFilter(), Ordered.HIGHEST_PRECEDENCE + 1);
    }

    @Bean
    public FilterRegistrationBean gzipBodyRequestFilter() {
        return createFilterRegistrationBean(
                new GzipBodyRequestFilter(jsonConfig.jacksonObjectMapper()), Ordered.HIGHEST_PRECEDENCE + 2);
    }

    @Bean
    public FilterRegistrationBean monitoringRequestFilter() {
        return createFilterRegistrationBean(new MonitoringRequestFilter(METRIC_REGISTRY), Ordered.HIGHEST_PRECEDENCE);
    }

    @Bean
    public MappingJackson2HttpMessageConverter mappingJackson2HttpMessageConverter() {
        final MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setObjectMapper(jsonConfig.jacksonObjectMapper());
        return converter;
    }

    @Override
    protected void configureMessageConverters(final List<HttpMessageConverter<?>> converters) {
        final StringHttpMessageConverter stringConverter = new StringHttpMessageConverter();
        stringConverter.setWriteAcceptCharset(false);

        converters.add(new ByteArrayHttpMessageConverter());
        converters.add(stringConverter);
        converters.add(new ResourceHttpMessageConverter());
        converters.add(new SourceHttpMessageConverter<>());

        converters.add(mappingJackson2HttpMessageConverter());
        super.configureMessageConverters(converters);
    }

    @Override
    protected void addArgumentResolvers(List<HandlerMethodArgumentResolver> argumentResolvers) {
        argumentResolvers.add(new ClientResolver(securitySettings, featureToggleService));
    }

    @Override
    public RequestMappingHandlerMapping requestMappingHandlerMapping() {
        final RequestMappingHandlerMapping handlerMapping = super.requestMappingHandlerMapping();
        handlerMapping.setUseSuffixPatternMatch(false);
        return handlerMapping;
    }

    private FilterRegistrationBean createFilterRegistrationBean(final Filter filter, final int order) {
        final FilterRegistrationBean filterRegistrationBean = new FilterRegistrationBean();
        filterRegistrationBean.setFilter(filter);
        filterRegistrationBean.setOrder(order);
        return filterRegistrationBean;
    }

}
