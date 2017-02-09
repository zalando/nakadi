package org.zalando.nakadi.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;

public class PrincipalMockFactory {

    public static Principal mockPrincipal(final String clientId) {
        final Principal principal = mock(Principal.class);
        when(principal.getName()).thenReturn(clientId);
        return principal;
    }

}
