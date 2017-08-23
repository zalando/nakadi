package org.zalando.nakadi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.AdminAuthorization;
import org.zalando.nakadi.exceptions.runtime.InsufficientAuthorizationException;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

@RestController
@RequestMapping("/settings/admins")
public class AdminController {

    private static final Logger LOG = LoggerFactory.getLogger(AdminController.class);
    private final AuthorizationValidator authorizationValidator;

    @Autowired
    public AdminController(final AuthorizationValidator authorizationValidator) {
        this.authorizationValidator = authorizationValidator;
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> getAdmins() {
        return ResponseEntity.ok(authorizationValidator.getAdmins());
    }

    @RequestMapping(method = RequestMethod.PUT)
    public ResponseEntity<?> updateAdmins(@RequestBody final AdminAuthorization authz) {
        authorizationValidator.updateAdmins(authz);
        return ResponseEntity.ok().build();
    }

    @ExceptionHandler(InsufficientAuthorizationException.class)
    public ResponseEntity<Problem> handleInsufficientAuthorizationException(final RuntimeException ex,
                                                                final NativeWebRequest request) {
        LOG.debug(ex.getMessage(), ex);
        return Responses.create(MoreStatus.UNPROCESSABLE_ENTITY, ex.getMessage(), request);
    }

}
