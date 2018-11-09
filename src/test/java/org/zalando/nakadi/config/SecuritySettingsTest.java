package org.zalando.nakadi.config;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class SecuritySettingsTest {

    @Test
    public void doesNotRequireAuthentication() {
        Assert.assertTrue(SecuritySettings.AuthMode.NONE.isNoAuthentication());
        Assert.assertTrue(SecuritySettings.AuthMode.OFF.isNoAuthentication());
    }

    @Test
    public void mustRequireAuthentication() {
        final List<SecuritySettings.AuthMode> authModesWithoutAuthentication =
                new LinkedList<>(Arrays.asList(SecuritySettings.AuthMode.values()));
        authModesWithoutAuthentication.remove(SecuritySettings.AuthMode.NONE);
        authModesWithoutAuthentication.remove(SecuritySettings.AuthMode.OFF);
        authModesWithoutAuthentication.forEach(authMode ->
                Assert.assertFalse(authMode.isNoAuthentication()));
    }

}