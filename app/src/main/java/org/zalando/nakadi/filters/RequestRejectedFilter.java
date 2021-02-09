package org.zalando.nakadi.filters;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.security.web.firewall.RequestRejectedException;
import org.springframework.stereotype.Component;
import javax.servlet.http.HttpServletResponse;

@Aspect
@Component
public class RequestRejectedFilter {

    @Around("execution(public void org.springframework.security.web.FilterChainProxy.doFilter(..))")
    public void handleRequestRejectedException(ProceedingJoinPoint pjp) throws Throwable {
        try {
            pjp.proceed();
        } catch (RequestRejectedException exception) {
            HttpServletResponse response = (HttpServletResponse) pjp.getArgs()[1];
            response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        }
    }
}
