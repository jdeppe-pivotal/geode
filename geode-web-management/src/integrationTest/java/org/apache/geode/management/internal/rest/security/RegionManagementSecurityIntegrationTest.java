/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.management.internal.rest.security;

import static org.hamcrest.Matchers.is;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.GenericXmlWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.context.web.WebMergedContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.GenericWebApplicationContext;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.internal.cache.HttpService;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-management-servlet.xml"},
    loader = TestContextLoader.class)
@WebAppConfiguration
public class RegionManagementSecurityIntegrationTest {

  private static RequestPostProcessor POST_PROCESSOR = new StandardRequestPostProcessor();
  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule()
      .withSecurityManager(SimpleTestSecurityManager.class)
      .withAutoStart();

  @ClassRule
  public static RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  private MockMvc mockMvc;

  private RegionConfig regionConfig;
  private String json;

  @Autowired
  private WebApplicationContext webApplicationContext;

  @Before
  public void before() throws JsonProcessingException {
    regionConfig = new RegionConfig();
    regionConfig.setName("customers");
    regionConfig.setType("REPLICATE");
    ObjectMapper mapper = new ObjectMapper();
    json = mapper.writeValueAsString(regionConfig);
    mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext)
        .apply(springSecurity())
        .build();
  }

  @Test
  public void sanityCheck_not_authorized() throws Exception {
    mockMvc.perform(post("/v2/regions")
        .with(httpBasic("user", "user"))
        .with(POST_PROCESSOR)
        .content(json))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.persistenceStatus.status", is("FAILURE")))
        .andExpect(jsonPath("$.persistenceStatus.message",
            is("user not authorized for DATA:MANAGE")));
  }

  @Test
  public void sanityCheckWithNoCredentials() throws Exception {
    mockMvc.perform(post("/v2/regions")
        .with(POST_PROCESSOR)
        .content(json))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.persistenceStatus.status", is("FAILURE")))
        .andExpect(jsonPath("$.persistenceStatus.message",
            is("Full authentication is required to access this resource")));
  }

  @Test
  public void sanityCheckWithWrongCredentials() throws Exception {
    mockMvc.perform(post("/v2/regions")
        .with(httpBasic("user", "wrong_password"))
        .with(POST_PROCESSOR)
        .content(json))
        .andDo(print())
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.persistenceStatus.status", is("FAILURE")))
        .andExpect(jsonPath("$.persistenceStatus.message",
            is("Authentication error. Please check your credentials.")));
  }

  @Test
  public void sanityCheck_success() throws Exception {
    mockMvc.perform(post("/v2/regions")
        .with(httpBasic("dataManage", "dataManage"))
        .with(POST_PROCESSOR)
        .content(json))
        .andDo(print())
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.persistenceStatus.status", is("FAILURE")))
        .andExpect(jsonPath("$.persistenceStatus.message",
            is("no members found to create cache element")));
  }

  private static class StandardRequestPostProcessor implements RequestPostProcessor {
    @Override
    public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
      request.addHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
      request.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
      return request;
    }
  }
}


class TestContextLoader extends GenericXmlWebContextLoader {
  @Override
  protected void loadBeanDefinitions(GenericWebApplicationContext context,
      WebMergedContextConfiguration webMergedConfig) {
    super.loadBeanDefinitions(context, webMergedConfig);
    context.getServletContext().setAttribute(HttpService.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM,
        RegionManagementSecurityIntegrationTest.locator.getCache().getSecurityService());
    context.getServletContext().setAttribute(HttpService.CLUSTER_MANAGEMENT_SERVICE_CONTEXT_PARAM,
        RegionManagementSecurityIntegrationTest.locator.getLocator().getClusterManagementService());
  }

}
