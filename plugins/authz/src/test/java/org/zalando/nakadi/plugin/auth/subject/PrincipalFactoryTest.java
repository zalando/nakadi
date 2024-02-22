package org.zalando.nakadi.plugin.auth.subject;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;
import org.zalando.nakadi.plugin.auth.OPAClient;
import org.zalando.nakadi.plugin.auth.ZalandoTeamService;

import java.util.Collections;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PrincipalFactoryTest {
    @Mock
    private OPAClient opaClient;
    @Mock
    private ZalandoTeamService teamService;
    private PrincipalFactory principalFactory;

    @Before
    public void setupPrincipalFactory() {
        principalFactory = new PrincipalFactory(
                "/employees",
                "users",
                "/services",
                "services",
                opaClient,
                teamService);
    }

    @Test
    public void verifyExternalSubjectParsingFromHeader() {
        final Set<String> expectedRetailerIds = Collections.singleton("TestValue");
        when(opaClient.getRetailerIdsForService(eq("merchant-api"))).thenReturn(expectedRetailerIds);

        final String knownBpid = "ahzhd657-dhsdjs-dshd83-dhsdjs";
        // Generated using (https://merchant-platform.docs.zalando.net/components/iam/#x-consumer-dev-tool)
        final String xConsumer = "eyJjbGllbnRfaWQiOiIiLCJncm91cHMiOlsiIl0sInNjb3BlcyI6WyIiXSwiYnBpZHMiOlsiYWh6aGQ2NTc" +
                "tZGhzZGpzLWRzaGQ4My1kaHNkanMiXX0";

        final ExternalSubject currentSubject = (ExternalSubject) principalFactory.createPrincipal(
                "merchant-api", xConsumer, "/services");
        assertTrue(currentSubject.getBpids().contains(knownBpid));
        assertEquals("merchant-api:business_partners:(" + knownBpid + ")", currentSubject.getName());

        assertEquals(currentSubject.getRetailerIdsToRead(), expectedRetailerIds);
    }

    @Test
    public void verifyInvalidRealmNotProcessed() {
        final PluginException e = assertThrows(
                PluginException.class,
                () -> principalFactory.createPrincipal("bla-bla", null, "unknown_realm"));
        assertEquals("Token has unknown realm: unknown_realm", e.getMessage());
    }

    @Test
    public void verifyUidSubjectCreation() {
        final Set<String> expectedRetailerIds = Collections.singleton("TestValue");
        when(opaClient.getRetailerIdsForService(eq("uid"))).thenReturn(expectedRetailerIds);

        final UidSubject uidSubject = (UidSubject) principalFactory.createPrincipal("uid", null, "/services");

        assertEquals("uid", uidSubject.getUid(), "uid");
        assertEquals("uid", uidSubject.getName());
        assertEquals(expectedRetailerIds, uidSubject.getRetailerIdsToRead());
    }

    @Test
    public void verifyUserSubjectCreation() {
        final Set<String> expectedRetailerIds = Collections.singleton("TestValue");
        when(opaClient.getRetailerIdsForUser(eq("uid"))).thenReturn(expectedRetailerIds);

        final UidSubject uidSubject = (UidSubject) principalFactory.createPrincipal("uid", null, "/employees");

        assertEquals("uid", uidSubject.getUid(), "uid");
        assertEquals("uid", uidSubject.getName());
        assertEquals(expectedRetailerIds, uidSubject.getRetailerIdsToRead());
    }
}