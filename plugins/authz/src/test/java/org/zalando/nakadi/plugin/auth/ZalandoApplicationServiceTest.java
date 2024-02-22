package org.zalando.nakadi.plugin.auth;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ZalandoApplicationServiceTest {
    @Mock
    private ValueRegistry applicationRegistry;

    private ZalandoApplicationService zalandoApplicationService;

    @Before
    public void setupBean() {
        zalandoApplicationService = new ZalandoApplicationService(applicationRegistry);
    }

    @Test
    public void verifyNullIsFiltered() {
        assertFalse(zalandoApplicationService.exists(null));
        verifyNoInteractions(applicationRegistry);
    }

    @Test
    public void verifySuccessStupsIsReplaced() {
        when(applicationRegistry.isValid("nakadi")).thenReturn(true);

        assertTrue(zalandoApplicationService.exists("stups_nakadi"));
    }

    @Test
    public void verifySuccessStupsIsNotReplaced() {
        when(applicationRegistry.isValid("stupsnakadi")).thenReturn(true);

        assertTrue(zalandoApplicationService.exists("stupsnakadi"));
    }

    @Test
    public void verifyFalseIsPropagated() {
        when(applicationRegistry.isValid("not-found")).thenReturn(false);
        assertFalse(zalandoApplicationService.exists("not-found"));
    }

    @Test
    public void verifyExceptionIsPropagated() {
        when(applicationRegistry.isValid(eq("exceptional"))).thenThrow(new PluginException("TestException"));
        final PluginException e = assertThrows(PluginException.class,
                () -> zalandoApplicationService.exists("exceptional"));
        assertEquals("TestException", e.getMessage());
    }


}