package org.zalando.nakadi.plugin.auth.attribute;

import org.junit.Assert;
import org.junit.Test;

public class TeamAuthorizationAttributeTest {
    @Test
    public void isTeamAuthorizationAttribute() {
        Assert.assertFalse(TeamAuthorizationAttribute.isTeamAuthorizationAttribute(
                new SimpleAuthorizationAttribute("user", "some-user")));
        Assert.assertTrue(TeamAuthorizationAttribute.isTeamAuthorizationAttribute(
                new SimpleAuthorizationAttribute("team", "someteam")));
    }
}