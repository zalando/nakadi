package org.zalando.nakadi.service;

import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.cache.SubscriptionCache;
import org.zalando.nakadi.domain.ResourceImpl;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class CursorsServiceTest {

    private AuthorizationValidator authorizationValidator;
    private CursorsService service;

    @Before
    public void setup() {
        authorizationValidator = mock(AuthorizationValidator.class);
        service = new CursorsService(mock(SubscriptionDbRepository.class), mock(SubscriptionCache.class), null,
                null, null, null, null, authorizationValidator, null);
    }

    @Test(expected = AccessDeniedException.class)
    public void whenResetCursorsThenAdminAccessChecked() {
        doThrow(new AccessDeniedException(AuthorizationService.Operation.ADMIN,
                new ResourceImpl<Subscription>("", ResourceImpl.SUBSCRIPTION_RESOURCE, null, null)))
                .when(authorizationValidator).authorizeSubscriptionAdmin(any());
        service.resetCursors("test", Collections.emptyList());
    }

    @Test(expected = AccessDeniedException.class)
    public void whenCommitCursorsAccessDenied() {
        doThrow(new AccessDeniedException(AuthorizationService.Operation.ADMIN,
                new ResourceImpl<Subscription>("", ResourceImpl.SUBSCRIPTION_RESOURCE, null, null)))
                .when(authorizationValidator).authorizeSubscriptionCommit(any());
        service.commitCursors("test", "test", Collections.emptyList());
    }
}
