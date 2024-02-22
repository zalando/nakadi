package org.zalando.nakadi.plugin.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.zalando.problem.ProblemModule;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class XConsumerHeaderFilterTest {
    final MerchantGatewayService merchantGatewayService = mock(MerchantGatewayService.class);
    final XConsumerHeaderFilter filter = new XConsumerHeaderFilter(merchantGatewayService,
            Arrays.asList("merchant-uid"), "event-streams/read",
            new ProblemWriter(new ObjectMapper().registerModule(new ProblemModule())));

    final HttpServletRequest request = mock(HttpServletRequest.class);
    final HttpServletResponse response = mock(HttpServletResponse.class);
    final FilterChain filterChain = mock(FilterChain.class);

    static final String X_CONSUMER_BASE64 = "ewogICJzY29wZXMiOiBbCiAgICAgIm9yZGVycy9yZWFkIiwKICAgICAiZXZlbnQtc3RyZWFt" +
            "cy9yZWFkIiwKICAgICAiYXJ0aWNsZXMvZWFuLW1hdGNoIgogIF0sCiAgImJwaWRzIjogWwogICAgICI1NzgxMy0zOTcyMS1iNDY3MC1k" +
            "NWY4OSIsCiAgICAgIjZmYjY5LTM5NzIxLWU4YjQ2LTBlZDVmIgogIF0sCiAgImNsaWVudF9pZCI6ICJ3ZWItcG9ydGFsIiwKICAiYWNj" +
            "b3VudF9pZCI6ICIxMGEyNmQyNy05ZGVkLTQ5OWQtOTM1Ny1lMDcxYjkwNWM5ZjgiLAogICJncm91cHMiOiBbICJ6YWxhbmRvL2RldmVs" +
            "b3BlciIgXSwKICAidXNlcl9pZCI6ICIxMGEyNmQyNy05ZGVkLTQ5OWQtOTM1Ny1lMDcxYjkwNWM5ZjgiCn0=";
    static final String X_CONSUMER_WRONG_SCOPE_BASE64 = "ewogICJzY29wZXMiOiBbCiAgICAgIm9yZGVycy9yZWFkIiwKICAgICAib3Jk" +
            "ZXJzL3VwZGF0ZSIsCiAgICAgImFydGljbGVzL2Vhbi1tYXRjaCIKICBdLAogICJicGlkcyI6IFsKICAgICAiNTc4MTMtMzk3MjEtYjQ2" +
            "NzAtZDVmODkiLAogICAgICI2ZmI2OS0zOTcyMS1lOGI0Ni0wZWQ1ZiIKICBdLAogICJjbGllbnRfaWQiOiAid2ViLXBvcnRhbCIsCiAg" +
            "ImFjY291bnRfaWQiOiAiMTBhMjZkMjctOWRlZC00OTlkLTkzNTctZTA3MWI5MDVjOWY4IiwKICAiZ3JvdXBzIjogWyAiemFsYW5kby9k" +
            "ZXZlbG9wZXIiIF0sCiAgInVzZXJfaWQiOiAiMTBhMjZkMjctOWRlZC00OTlkLTkzNTctZTA3MWI5MDVjOWY4Igp9";

    @Test
    public void testWhenNoXConsumerHeaderThenDoNotVerifySignature() throws ServletException, IOException {
        when(request.getHeader("X-Consumer")).thenReturn(null);
        when(request.getMethod()).thenReturn("GET");
        when(request.getPathInfo()).thenReturn("/event-types/my-event-type");
        when(request.getServletPath()).thenReturn("");
        final Authentication authentication = mock(OAuth2Authentication.class);
        when(authentication.getPrincipal()).thenReturn("merchant-uid");
        SecurityContextHolder.getContext().setAuthentication(authentication);
        when(((OAuth2Authentication) authentication).getUserAuthentication()).thenReturn(authentication);
        when(authentication.getDetails()).thenReturn("details");

        filter.doFilterInternal(request, response, filterChain);

        verify(merchantGatewayService, never()).isSignatureValid(any());
    }

    @Test
    public void testBlockedEndpointForGateway() throws ServletException, IOException {
        when(request.getMethod()).thenReturn("GET");
        when(request.getPathInfo()).thenReturn("/event-types");
        when(request.getServletPath()).thenReturn("");
        final StringWriter writer = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(writer));
        final Authentication authentication = mock(OAuth2Authentication.class);
        when(authentication.getPrincipal()).thenReturn("merchant-uid");
        SecurityContextHolder.getContext().setAuthentication(authentication);

        filter.doFilterInternal(request, response, filterChain);

        verify(filterChain, never()).doFilter(request, response);
        assertEquals("{\"title\":\"Unauthorized\",\"status\":401," +
                        "\"detail\":\"Access to this endpoint is not allowed through Merchant Gateway\"}",
                writer.getBuffer().toString());
    }

    @Test
    public void testNotBlockedEndpointForGateway() throws ServletException, IOException {
        when(request.getMethod()).thenReturn("GET");
        when(request.getPathInfo()).thenReturn("/event-types/my-event-type");
        when(request.getServletPath()).thenReturn("");
        final Authentication authentication = mock(OAuth2Authentication.class);
        when(authentication.getPrincipal()).thenReturn("merchant-uid");
        SecurityContextHolder.getContext().setAuthentication(authentication);
        when(((OAuth2Authentication) authentication).getUserAuthentication()).thenReturn(authentication);
        when(authentication.getDetails()).thenReturn("details");

        filter.doFilterInternal(request, response, filterChain);

        verify(filterChain, times(1)).doFilter(request, response);
    }

    @Test
    public void testWhenXConsumerIsPresentThenVerifySignature() throws ServletException, IOException {
        when(merchantGatewayService.isValidJsonWhenDecoded(any())).thenReturn(true);
        // base64-encoded example from merchant platform documentation, with second scope replaced
        // with 'event-streams/read'
        when(request.getHeader("X-Consumer")).thenReturn(X_CONSUMER_BASE64);
        when(request.getHeader("X-Consumer-Signature")).thenReturn("some-signature");
        when(request.getHeader("X-Consumer-Key-ID")).thenReturn("some-key-id");
        when(request.getPathInfo()).thenReturn("/event-types/my-event-type");

        final StringWriter writer = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(writer));

        when(merchantGatewayService.isSignatureValid(any())).thenReturn(true);

        filter.doFilterInternal(request, response, filterChain);

        verify(merchantGatewayService, times(1)).isSignatureValid(any());
    }

    @Test
    public void testWhenSignatureInvalidBreakChainCall() throws ServletException, IOException {
        when(merchantGatewayService.isValidJsonWhenDecoded(any())).thenReturn(true);
        when(request.getHeader("X-Consumer")).thenReturn("some-content");
        when(request.getHeader("X-Consumer-Signature")).thenReturn("some-signature");
        when(request.getHeader("X-Consumer-Key-ID")).thenReturn("some-key-id");

        final StringWriter writer = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(writer));

        when(merchantGatewayService.isSignatureValid(any())).thenReturn(false);

        filter.doFilterInternal(request, response, filterChain);

        verify(filterChain, never()).doFilter(request, response);

        assertEquals(writer.getBuffer().toString(), "{\"title\":\"Unauthorized\",\"status\":401," +
                "\"detail\":\"Problem verifying request origin\"}");
    }

    @Test
    public void testWhenSignatureIsValidContinueChainCall() throws ServletException, IOException {
        when(merchantGatewayService.isValidJsonWhenDecoded(any())).thenReturn(true);
        when(request.getHeader("X-Consumer")).thenReturn(X_CONSUMER_BASE64);
        when(request.getHeader("X-Consumer-Signature")).thenReturn("some-signature");
        when(request.getHeader("X-Consumer-Key-ID")).thenReturn("some-key-id");

        final Authentication auth = mock(Authentication.class);
        when(auth.getPrincipal()).thenReturn("user");
        SecurityContextHolder.getContext().setAuthentication(auth);

        final StringWriter writer = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(writer));

        when(merchantGatewayService.isSignatureValid(any())).thenReturn(true);

        filter.doFilterInternal(request, response, filterChain);

        verify(filterChain, times(1)).doFilter(request, response);
    }

    @Test
    public void testWhenInvalidJSONBreakChainCall() throws ServletException, IOException {
        when(request.getHeader("X-Consumer")).thenReturn("some-content"); // invalid x-consumer content
        when(request.getHeader("X-Consumer-Signature")).thenReturn("some-signature");
        when(request.getHeader("X-Consumer-Key-ID")).thenReturn("some-key-id");

        final Authentication auth = mock(Authentication.class);
        when(auth.getPrincipal()).thenReturn("user");
        SecurityContextHolder.getContext().setAuthentication(auth);

        final StringWriter writer = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(writer));
        filter.doFilterInternal(request, response, filterChain);

        verify(filterChain, never()).doFilter(request, response);

        assertEquals(writer.getBuffer().toString(), "{\"title\":\"Bad Request\",\"status\":400," +
                "\"detail\":\"Invalid request origin parameters\"}");
    }

    @Test
    public void testWhenMissingScopeBreakChainCall() throws ServletException, IOException {
        when(request.getHeader("X-Consumer")).thenReturn(X_CONSUMER_WRONG_SCOPE_BASE64);
        when(request.getHeader("X-Consumer-Signature")).thenReturn("some-signature");
        when(request.getHeader("X-Consumer-Key-ID")).thenReturn("some-key-id");
        when(request.getPathInfo()).thenReturn("/event-types/my-event-type");

        final StringWriter writer = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(writer));

        when(merchantGatewayService.isValidJsonWhenDecoded(any())).thenReturn(true);
        when(merchantGatewayService.isSignatureValid(any())).thenReturn(true);

        filter.doFilterInternal(request, response, filterChain);

        assertEquals("{\"title\":\"Unauthorized\",\"status\":401," +
                "\"detail\":\"Insufficient scopes for this operation\"}", writer.getBuffer().toString());
    }
}
