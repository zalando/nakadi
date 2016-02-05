package de.zalando.aruha.nakadi.controller;

import com.google.common.base.CaseFormat;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.zalando.problem.spring.web.advice.ProblemHandling;

@ControllerAdvice
public final class ExceptionHandling implements ProblemHandling {

    @Override
    public String formatFieldName(String fieldName) {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, fieldName);
    }

}
