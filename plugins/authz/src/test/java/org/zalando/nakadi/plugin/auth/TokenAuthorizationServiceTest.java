package org.zalando.nakadi.plugin.auth;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.exceptions.AuthorizationInvalidException;
import org.zalando.nakadi.plugin.api.exceptions.OperationOnResourceNotPermittedException;
import org.zalando.nakadi.plugin.auth.attribute.SimpleAuthorizationAttribute;
import org.zalando.nakadi.plugin.auth.subject.EmployeeSubject;
import org.zalando.nakadi.plugin.auth.subject.Principal;
import org.zalando.nakadi.plugin.auth.utils.SimpleEventResource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.plugin.auth.ResourceType.ALL_DATA_ACCESS_RESOURCE;
import static org.zalando.nakadi.plugin.auth.utils.ResourceBuilder.rb;

@RunWith(MockitoJUnitRunner.class)
public class TokenAuthorizationServiceTest {

    private static final String BUSINESS_PARTNER_TYPE = "business_partner";
    public static final String SERVICES_TYPE = "services";
    public static final String USERS_TYPE = "users";

    private TokenAuthorizationService authzService;
    @Mock
    private Authentication authentication;
    @Mock
    private TokenProvider tokenProvider;
    @Mock
    private ValueRegistry userRegistry;
    @Mock
    private ValueRegistry serviceRegistry;
    @Mock
    private ValueRegistry merchantRegistry;
    @Mock
    private Principal principal;
    @Mock
    private ZalandoTeamService teamService;

    @Before
    public void setUp() {
        when(authentication.getPrincipal()).thenReturn(principal);

        authzService = new TokenAuthorizationService(
                USERS_TYPE, userRegistry,
                SERVICES_TYPE, serviceRegistry,
                BUSINESS_PARTNER_TYPE, merchantRegistry,
                teamService,
                Arrays.asList("stups_merchant-uid"));

        SecurityContextHolder.getContext().setAuthentication(new OAuth2Authentication(null, authentication));
    }

    @Test
    public void serviceWithoutStupsPrefixDoesNotExist() {
        final Resource<?> r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, SERVICES_TYPE, "nakadi")
                .build();

        when(principal.isExternal()).thenReturn(false);
        when(serviceRegistry.isValid(anyString())).thenReturn(true);

        // serviceRegistry should be called, so that mockito is not reporting about isValid not called.
        authzService.isAuthorizationForResourceValid(rb("warmup-mockito", "event-type")
                .add(AuthorizationService.Operation.READ, SERVICES_TYPE, "stups_nakadi")
                .build());

        final AuthorizationInvalidException e = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));

        assertThat(e.getMessage(),
                equalTo("authorization attribute services:nakadi is invalid"));
    }

    @Test
    public void testServiceValidationExecuted() {
        final Resource<?> r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, SERVICES_TYPE, "stups_idonotexist")
                .build();
        when(principal.isExternal()).thenReturn(false);

        when(serviceRegistry.isValid(eq("idonotexist"))).thenReturn(false);
        assertThrows(AuthorizationInvalidException.class, () -> authzService.isAuthorizationForResourceValid(r));

        when(serviceRegistry.isValid(eq("idonotexist"))).thenReturn(true);
        authzService.isAuthorizationForResourceValid(r);
    }

    @Test
    public void serviceWithUUIDIsValid() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, SERVICES_TYPE, "eda0f39a-8b22-48d8-b9f9-aed62f15e2cf")
                .build();

        when(principal.isExternal()).thenReturn(false);

        authzService.isAuthorizationForResourceValid(r);
    }

    @Test
    public void gatewayServiceIsNotValid() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, SERVICES_TYPE, "stups_merchant-uid")
                .build();
        when(serviceRegistry.isValid(eq("merchant-uid"))).thenReturn(true);

        final AuthorizationInvalidException e = assertThrows(
                AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(),
                equalTo("Gateway is not allowed in authorization section"));
    }

    @Test
    public void whenAuthCallSucceedsThenSuccess() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, USERS_TYPE, "user1")
                .build();
        when(
                principal.isAuthorized(
                        eq("event-type"),
                        eq(AuthorizationService.Operation.READ),
                        eq(Optional.of(Arrays.asList(new SimpleAuthorizationAttribute(USERS_TYPE, "user1"))))))
                .thenReturn(true);

        assertTrue(authzService.isAuthorized(AuthorizationService.Operation.READ, r));
    }

    @Test
    public void whenPrincipalOauthCallFailsThenFail() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, USERS_TYPE, "user1")
                .build();
        when(
                principal.isAuthorized(
                        eq("event-type"),
                        eq(AuthorizationService.Operation.READ),
                        eq(Optional.of(Arrays.asList(new SimpleAuthorizationAttribute(USERS_TYPE, "user1"))))))
                .thenReturn(false);

        assertFalse(authzService.isAuthorized(AuthorizationService.Operation.READ, r));
    }

    @Test
    public void testBusinessPartnerIsValid() {
        final Resource r = rb("myResource1", "event-type")
                .resource(new SimpleEventResource("COMPATIBLE", "DELETE"))
                .add(AuthorizationService.Operation.READ, BUSINESS_PARTNER_TYPE, "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .build();
        when(merchantRegistry.isValid(eq("ahzhd657-dhsdjs-dshd83-dhsdjs"))).thenReturn(true);
        authzService.isAuthorizationForResourceValid(r);
    }

    @Test
    public void testBPDoesNotOccurWithWildCardForEventType() throws Exception {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.ADMIN, "business_partner", "abcde")
                .add(AuthorizationService.Operation.READ, "*", "*")
                .resource(new SimpleEventResource("COMPATIBLE", "DELETE"))
                .build();
        when(merchantRegistry.isValid(eq("abcde"))).thenReturn(true);

        final AuthorizationInvalidException e = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(),
                equalTo("Business Partner cannot be present with wild card in authorization"));
    }

    @Test
    public void testAuthorizationHasAtMostOneBPForOneOperationForEventType() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, "business_partner", "abcde")
                .add(AuthorizationService.Operation.READ, "business_partner", "abc")
                .resource(new SimpleEventResource("COMPATIBLE", "DELETE"))
                .build();
        when(merchantRegistry.isValid(any())).thenReturn(true);

        final AuthorizationInvalidException e = assertThrows(
                AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(), equalTo("Only one business partner allowed for each operation"));
    }

    @Test
    public void testSuccessfulCreationForSubscription() {
        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.READ, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .add(AuthorizationService.Operation.ADMIN, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .build();

        when(principal.isExternal()).thenReturn(true);
        when(principal.getBpids()).thenReturn(Collections.singleton("ahzhd657-dhsdjs-dshd83-dhsdjs"));
        when(merchantRegistry.isValid(eq("ahzhd657-dhsdjs-dshd83-dhsdjs"))).thenReturn(true);

        authzService.isAuthorizationForResourceValid(r);
    }

    @Test
    public void testSameBPInAdminAndReaderForSubscription() {
        when(principal.isExternal()).thenReturn(true);
        when(principal.getBpids()).thenReturn(Collections.singleton("ahzhd657-dhsdjs-dshd83-dhsdjs"));
        when(merchantRegistry.isValid(eq("ahzhd657-dhsdjs-dshd83-dhsdjs"))).thenReturn(true);
        when(merchantRegistry.isValid(eq("abc"))).thenReturn(true);

        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.READ, "business_partner", "abc")
                .add(AuthorizationService.Operation.ADMIN, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .build();
        final AuthorizationInvalidException e1 = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e1.getMessage(),
                equalTo("Business partner must only add itself as both admin and reader"));
    }

    @Test
    public void testSameOneBPInAdminAndReaderForSubscription() {
        when(principal.isExternal()).thenReturn(true);
        when(principal.getBpids()).thenReturn(Collections.singleton("ahzhd657-dhsdjs-dshd83-dhsdjs"));
        when(merchantRegistry.isValid(eq("ahzhd657-dhsdjs-dshd83-dhsdjs"))).thenReturn(true);

        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.READ, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .add(AuthorizationService.Operation.READ, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .add(AuthorizationService.Operation.ADMIN, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .build();

        final AuthorizationInvalidException e2 = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e2.getMessage(),
                equalTo("Business partner must only add itself as both admin and reader"));
    }

    @Test
    public void testEmptyAuthorizationForBP() {
        final Resource r = rb("myResource1", "event-type").build();
        when(principal.isExternal()).thenReturn(true);

        final AuthorizationInvalidException e = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(), equalTo("Empty authorization is not allowed"));
    }

    @Test
    public void testZalandoEmployeeCannotCreateSubscription() {
        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.READ, BUSINESS_PARTNER_TYPE, "abcde")
                .build();

        when(merchantRegistry.isValid(any())).thenReturn(true);
        when(principal.isExternal()).thenReturn(false);

        final OperationOnResourceNotPermittedException e = assertThrows(OperationOnResourceNotPermittedException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(), equalTo("Subscription including business " +
                "partner can be only created by corresponding business partner"));
    }

    @Test
    public void testBPCanCreateSubscriptionWithValidCredentials() {
        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.READ, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .add(AuthorizationService.Operation.ADMIN, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .build();
        when(principal.getBpids()).thenReturn(Collections.singleton("ahzhd657-dhsdjs-dshd83-dhsdjs"));
        when(principal.isExternal()).thenReturn(true);
        when(merchantRegistry.isValid(any())).thenReturn(true);

        authzService.isAuthorizationForResourceValid(r);
    }

    @Test
    public void testBPCanNotCreateSubscriptionWithInvalidCredentials() {
        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.ADMIN, "services", "stups_nakadi")
                .add(AuthorizationService.Operation.READ, "*", "*")
                .build();
        when(principal.isExternal()).thenReturn(true);
        when(serviceRegistry.isValid(any())).thenReturn(true);

        final AuthorizationInvalidException e = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(),
                equalTo("Authorization should contain business partner"));
    }

    @Test
    public void testZalandoEmployeeAuthorisationHasMinimumRestriction() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, "services", "stups_nakadi")
                .build();

        when(serviceRegistry.isValid(eq("nakadi"))).thenReturn(true);
        when(principal.isExternal()).thenReturn(false);

        authzService.isAuthorizationForResourceValid(r);

        //For Null Authorization
        authzService.isAuthorizationForResourceValid(rb("myResource1", "event-type").build());
    }

    @Test
    public void testBPCannotAccessAllDataAndPermissionResource() {
        final Resource r = rb("myResource1", ALL_DATA_ACCESS_RESOURCE)
                .add(AuthorizationService.Operation.READ, BUSINESS_PARTNER_TYPE, "abcde")
                .add(AuthorizationService.Operation.WRITE, BUSINESS_PARTNER_TYPE, "abcde")
                .build();
        when(merchantRegistry.isValid("abcde")).thenReturn(true);

        final OperationOnResourceNotPermittedException e = assertThrows(OperationOnResourceNotPermittedException.class,
                () -> authzService.isAuthorizationForResourceValid(r));

        assertThat(e.getMessage(),
                equalTo("Business Partner is not allowed access to the resource"));
    }

    @Test
    public void testBusinessPartnerIsNotFound() {
        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.ADMIN, BUSINESS_PARTNER_TYPE, "abcde")
                .build();
        when(merchantRegistry.isValid("abcde")).thenReturn(false);

        final AuthorizationInvalidException e = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(),
                equalTo("authorization attribute business_partner:abcde is invalid"));
    }

    @Test
    public void testTeamMemberIsAuthorized() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.WRITE, "team", "aruha")
                .build();
        when(teamService.getTeamMembers("aruha"))
                .thenReturn(Collections.singletonList("auser"));

        when(authentication.getPrincipal())
                .thenReturn(new EmployeeSubject("jdoe", Collections::emptySet, "users", teamService));
        assertFalse("jdoe should not be authorized",
                authzService.isAuthorized(AuthorizationService.Operation.WRITE, r));

        when(authentication.getPrincipal())
                .thenReturn(new EmployeeSubject("auser", Collections::emptySet, "users", teamService));
        assertTrue("auser should be authorized",
                authzService.isAuthorized(AuthorizationService.Operation.WRITE, r));
    }

    @Test
    public void testTeamIsAllowedInAuthorization() {
        final AuthorizationAttribute attribute = new SimpleAuthorizationAttribute("team", "aruha");
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, attribute)
                .build();

        when(teamService.isValidTeam(eq(attribute.getValue()))).thenReturn(true);
        when(principal.isExternal()).thenReturn(false);
        authzService.isAuthorizationForResourceValid(r);
    }

    @Test
    public void testWhenOnlyEmptyTeamInAuthorizationThenNotAuthorized() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.WRITE, "team", "empty-team")
                .build();
        when(teamService.getTeamMembers("empty-team"))
                .thenReturn(Collections.emptyList());

        when(authentication.getPrincipal())
                .thenReturn(new EmployeeSubject("jdoe", Collections::emptySet, "users", teamService));
        assertFalse("jdoe should not be authorized",
                authzService.isAuthorized(AuthorizationService.Operation.WRITE, r));
    }
}
