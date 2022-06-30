package org.zalando.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.xml.SourceHttpMessageConverter;
import org.springframework.security.web.context.AbstractSecurityWebApplicationInitializer;
import org.springframework.web.context.request.async.TimeoutCallableProcessingInterceptor;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;
import org.zalando.nakadi.filters.ExtraTracingFilter;
import org.zalando.nakadi.filters.LoggingFilter;
import org.zalando.nakadi.filters.MonitoringRequestFilter;
import org.zalando.nakadi.filters.RequestRejectedFilter;
import org.zalando.nakadi.filters.TracingFilter;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;
import org.zalando.nakadi.util.CompressionBodyRequestFilter;
import org.zalando.nakadi.util.FlowIdRequestFilter;

import javax.servlet.Filter;
import java.util.List;

@Configuration
public class WebConfig extends WebMvcConfigurationSupport {

    @Value("${nakadi.stream.timeoutMs}")
    private long nakadiStreamTimeout;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ClientResolver clientResolver;
    @Autowired
    private AuthorizationService authorizationService;
    @Autowired
    private NakadiKpiPublisher nakadiKpiPublisher;
    @Autowired
    private FeatureToggleService featureToggleService;

    @Autowired
    private MetricRegistry metricRegistry;

    @Autowired
    @Qualifier("perPathMetricRegistry")
    private MetricRegistry perPathMetricRegistry;

    @Autowired
    private AsyncTaskExecutor asyncTaskExecutor;

    @Override
    public void configureAsyncSupport(final AsyncSupportConfigurer configurer) {
        configurer.setDefaultTimeout(nakadiStreamTimeout);
        configurer.registerCallableInterceptors(timeoutInterceptor());
        configurer.setTaskExecutor(asyncTaskExecutor);
    }

    @Bean
    public TimeoutCallableProcessingInterceptor timeoutInterceptor() {
        return new TimeoutCallableProcessingInterceptor();
    }


    // ==========================================================================================
    // BEGIN FILTERS
    //
    // From the highest to the lowest precedence, in increments of 10 (SysV init-script style).
    //
    @Bean
    public FilterRegistrationBean traceRequestFilter() {
        return createFilterRegistrationBean(
                new TracingFilter(),
                Ordered.HIGHEST_PRECEDENCE);
    }

    @Bean
    public FilterRegistrationBean flowIdRequestFilter() {
        return createFilterRegistrationBean(
                new FlowIdRequestFilter(),
                Ordered.HIGHEST_PRECEDENCE + 10);
    }

    @Bean
    public FilterRegistrationBean requestRejectedFilter() {
        return createFilterRegistrationBean(
                new RequestRejectedFilter(),
                Ordered.HIGHEST_PRECEDENCE + 20);
    }

    // MIDDLE POINT
    @Bean
    public FilterRegistrationBean securityFilterChain(
            @Qualifier(AbstractSecurityWebApplicationInitializer.DEFAULT_FILTER_NAME) final Filter securityFilter) {
        //
        // Override the order of the Spring's security filter, which by default has the lowest
        // precedence.
        //
        // This is required so that the filters that are defined in the authz plugin are chained in
        // the correct order.
        //
        final FilterRegistrationBean filterRegistrationBean = createFilterRegistrationBean(securityFilter, 0);
        filterRegistrationBean.setName(AbstractSecurityWebApplicationInitializer.DEFAULT_FILTER_NAME);
        return filterRegistrationBean;
    }

    //
    // The authorization subject is populated by the authz plugin: therefore, set the order of any
    // filters that use AuthorizationService to be greater than 2.
    //
    @Bean
    public FilterRegistrationBean monitoringRequestFilter() {
        return createFilterRegistrationBean(
                new MonitoringRequestFilter(metricRegistry, perPathMetricRegistry, authorizationService),
                10);
    }

    @Bean
    public FilterRegistrationBean loggingFilter() {
        return createFilterRegistrationBean(
                new LoggingFilter(nakadiKpiPublisher, authorizationService, featureToggleService),
                20);
    }

    @Bean
    public FilterRegistrationBean gzipBodyRequestFilter(final ObjectMapper mapper) {
        return createFilterRegistrationBean(
                new CompressionBodyRequestFilter(mapper),
                Ordered.LOWEST_PRECEDENCE - 20);
    }

    @Bean
    public FilterRegistrationBean extraTraceRequestFilter() {
        return createFilterRegistrationBean(
                new ExtraTracingFilter(authorizationService),
                Ordered.LOWEST_PRECEDENCE - 10);
    }
    // END FILTERS
    // ==========================================================================================

    @Bean
    public MappingJackson2HttpMessageConverter mappingJackson2HttpMessageConverter() {
        final MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setObjectMapper(objectMapper);
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
    protected void addArgumentResolvers(final List<HandlerMethodArgumentResolver> argumentResolvers) {
        argumentResolvers.add(clientResolver);
    }

    @Override
    protected PathMatchConfigurer getPathMatchConfigurer() {
        final PathMatchConfigurer pathMatchConfigurer = super.getPathMatchConfigurer();
        pathMatchConfigurer.setUseSuffixPatternMatch(false);

        return pathMatchConfigurer;
    }

    @Override
    protected void configureContentNegotiation(final ContentNegotiationConfigurer configurer) {
        super.configureContentNegotiation(configurer);
        configurer.favorPathExtension(false);
    }

    private FilterRegistrationBean createFilterRegistrationBean(final Filter filter, final int order) {
        final FilterRegistrationBean filterRegistrationBean = new FilterRegistrationBean();
        filterRegistrationBean.setFilter(filter);
        filterRegistrationBean.setOrder(order);
        return filterRegistrationBean;
    }

}
