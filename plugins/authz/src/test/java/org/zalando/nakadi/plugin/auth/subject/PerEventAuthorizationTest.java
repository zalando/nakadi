package org.zalando.nakadi.plugin.auth.subject;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.auth.attribute.SimpleAuthorizationAttribute;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.zalando.nakadi.plugin.auth.ResourceType.EVENT_RESOURCE;

@RunWith(Parameterized.class)
public class PerEventAuthorizationTest {

    private final Principal principal;

    @Parameterized.Parameters
    public static Collection<Principal> principalsToCheck() {
        final Set<String> allowedToRead = new HashSet<>();
        allowedToRead.add("data_r1");
        allowedToRead.add("data_r2");

        return Arrays.asList(
                new ExternalSubject(
                        "test",
                        () -> allowedToRead,
                        Collections.emptySet()),
                new UidSubject(
                        "test",
                        () -> allowedToRead,
                        "test_type"
                )
        );
    }

    public PerEventAuthorizationTest(final Principal principal) {
        this.principal = principal;
    }

    @Test
    public void testReadChecked() {
        Assert.assertTrue(principal.isAuthorized(
                EVENT_RESOURCE,
                AuthorizationService.Operation.READ,
                Optional.of(Collections.singletonList(
                        new SimpleAuthorizationAttribute("retailer_id", "data_r1")))));

        Assert.assertTrue(principal.isAuthorized(
                EVENT_RESOURCE,
                AuthorizationService.Operation.READ,
                Optional.of(Collections.singletonList(
                        new SimpleAuthorizationAttribute("retailer_id", "data_r2")))));

        Assert.assertFalse(principal.isAuthorized(
                EVENT_RESOURCE,
                AuthorizationService.Operation.READ,
                Optional.of(Collections.singletonList(
                        new SimpleAuthorizationAttribute("retailer_id", "data_r3")))));

        Assert.assertFalse(principal.isAuthorized(
                EVENT_RESOURCE,
                AuthorizationService.Operation.READ,
                Optional.of(Collections.singletonList(
                        new SimpleAuthorizationAttribute("retailer_id", "data_w1")))));
    }

    @Test
    public void testWriteNotChecked() {
        Assert.assertTrue(principal.isAuthorized(
                EVENT_RESOURCE,
                AuthorizationService.Operation.WRITE,
                Optional.of(Collections.singletonList(
                        new SimpleAuthorizationAttribute("retailer_id", "data_w1")))));

        Assert.assertTrue(principal.isAuthorized(
                EVENT_RESOURCE,
                AuthorizationService.Operation.WRITE,
                Optional.of(Collections.singletonList(
                        new SimpleAuthorizationAttribute("retailer_id", "data_w2")))));

        Assert.assertTrue(principal.isAuthorized(
                EVENT_RESOURCE,
                AuthorizationService.Operation.WRITE,
                Optional.of(Collections.singletonList(
                        new SimpleAuthorizationAttribute("retailer_id", "data_r1")))));

    }
}