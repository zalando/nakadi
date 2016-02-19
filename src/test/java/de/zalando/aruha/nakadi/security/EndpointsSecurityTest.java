package de.zalando.aruha.nakadi.security;


import de.zalando.aruha.nakadi.Application;
import de.zalando.aruha.nakadi.config.NakadiConfig;
import de.zalando.aruha.nakadi.config.SecurityConfiguration;
import de.zalando.aruha.nakadi.config.WebConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import javax.servlet.Filter;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@WebAppConfiguration
public class EndpointsSecurityTest {

//    @Configuration
//    @Import(value = {NakadiConfig.class, SecurityConfiguration.class, WebConfig.class})
//    static class Config {
//
//    }

    @Autowired
    private WebApplicationContext wac;

    @Autowired
    private Filter springSecurityFilterChain;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        this.mockMvc = MockMvcBuilders.standaloneSetup().addFilter(SecurityMockMvcConfigurers.)
                .webAppContextSetup(wac)
                .addFilters(springSecurityFilterChain)
                .build();
    }

    @Test
    public void blah() throws Exception {
        mockMvc
                .perform(MockMvcRequestBuilders.get("/health"))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }
}
