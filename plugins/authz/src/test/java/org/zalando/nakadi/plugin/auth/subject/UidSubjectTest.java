package org.zalando.nakadi.plugin.auth.subject;

import org.junit.Test;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.auth.attribute.SimpleAuthorizationAttribute;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UidSubjectTest {

    @Test
    public void authorizeValidService() {
        final Principal principal = new UidSubject("stups_nakadi", Collections::emptySet, "services");

        assertTrue(principal.isAuthorized("myResource1", AuthorizationService.Operation.READ,
                Optional.of(Collections.singletonList(new SimpleAuthorizationAttribute("services", "stups_nakadi")))));
    }

    @Test
    public void rejectValidEmployeeWhenEmployeeNotAuthorized() {
        final Principal principal = new UidSubject("uid", Collections::emptySet, "users");

        assertFalse(
                principal.isAuthorized(
                        "event-type",
                        AuthorizationService.Operation.READ,
                        Optional.of(Arrays.asList(
                                new SimpleAuthorizationAttribute("users", "user2"),
                                new SimpleAuthorizationAttribute("users", "user3"),
                                new SimpleAuthorizationAttribute("services", "user1"))
                        )));
    }

    @Test
    public void authorizeValidEmployeeFromWildcard() {
        final Principal principal = new UidSubject("user1", Collections::emptySet, "users");
        assertTrue(
                principal.isAuthorized(
                        "event-type",
                        AuthorizationService.Operation.READ,
                        Optional.of(Collections.singletonList(new SimpleAuthorizationAttribute("*", "*")))));
    }


    @Test
    public void authorizeValidServiceFromWildcard() {
        final Principal principal = new UidSubject("stups_nakadi", Collections::emptySet, "services");
        assertTrue(
                principal.isAuthorized(
                        "event-type",
                        AuthorizationService.Operation.READ,
                        Optional.of(Collections.singletonList(new SimpleAuthorizationAttribute("*", "*")))));
    }

}
