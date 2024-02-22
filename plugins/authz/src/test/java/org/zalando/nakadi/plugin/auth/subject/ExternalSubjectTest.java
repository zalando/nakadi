package org.zalando.nakadi.plugin.auth.subject;

import org.junit.Test;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.auth.attribute.SimpleAuthorizationAttribute;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExternalSubjectTest {

    @Test
    public void authorizeOnlyValidBP() {
        final Principal p = new ExternalSubject(
                "merchant-api", Collections::emptySet, Collections.singleton("ahzhd657-dhsdjs-dshd83-dhsdjs"));

        assertTrue(p.isAuthorized("event-type", AuthorizationService.Operation.READ, Optional.of(
                Collections.singletonList(
                        new SimpleAuthorizationAttribute("business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs"))
        )));

        assertFalse(p.isAuthorized("event-type", AuthorizationService.Operation.READ, Optional.of(
                Collections.singletonList(
                        new SimpleAuthorizationAttribute("business_partner", "ahzhd657-dhsdjs-dshd83-dhaaajw"))
        )));
    }

    @Test
    public void verifyBusinessPartnerDeniedAuthorizationWithEmptyResourceAuth() {
        final Principal p = new ExternalSubject(
                "merchant-api", Collections::emptySet, Collections.singleton("ahzhd657-dhsdjs-dshd83-dhsdjs"));
        assertFalse(p.isAuthorized(
                "test", AuthorizationService.Operation.VIEW, Optional.empty()));
        assertFalse(p.isAuthorized(
                "test", AuthorizationService.Operation.VIEW, Optional.of(Collections.emptyList())));
    }

}