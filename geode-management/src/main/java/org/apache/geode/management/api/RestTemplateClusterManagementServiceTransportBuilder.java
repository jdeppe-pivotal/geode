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
 *
 */

package org.apache.geode.management.api;

import java.util.Arrays;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriTemplateHandler;

import org.apache.geode.management.internal.RestTemplateResponseErrorHandler;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class RestTemplateClusterManagementServiceTransportBuilder
    implements ClusterManagementServiceTransportBuilder {

  static final ResponseErrorHandler DEFAULT_ERROR_HANDLER =
      new RestTemplateResponseErrorHandler();

  private RestTemplate restTemplate = null;
  private ClusterManagementServiceConnectionConfig connectionConfig;

  public ClusterManagementServiceTransportBuilder setRestTemplate(RestTemplate template) {
    this.restTemplate = template;
    return this;
  }

  @Override
  public ClusterManagementServiceTransportBuilder setConnectionConfig(ClusterManagementServiceConnectionConfig connectionConfig) {
    this.connectionConfig = connectionConfig;
    return this;
  }

  @Override
  public ClusterManagementServiceTransport build() {
    if (connectionConfig == null) {
      throw new IllegalStateException("ConnectionConfig cannot be null. Please use setConnectionConfig()");
    }

    if (restTemplate == null) {
      restTemplate = new RestTemplate();
    }

    restTemplate.setErrorHandler(DEFAULT_ERROR_HANDLER);

    if (connectionConfig.getHost() == null || connectionConfig.getPort() <= 0) {
      throw new IllegalArgumentException(
          "host and port needs to be specified in order to build the service.");
    }

    DefaultUriTemplateHandler templateHandler = new DefaultUriTemplateHandler();
    String schema = (connectionConfig.getSslContext() == null) ? "http" : "https";
    templateHandler
        .setBaseUrl(schema + "://" + connectionConfig.getHost() + ":" + connectionConfig.getPort() + "/management");
    restTemplate.setUriTemplateHandler(templateHandler);

    // HttpComponentsClientHttpRequestFactory allows use to preconfigure httpClient for
    // authentication and ssl context
    HttpComponentsClientHttpRequestFactory requestFactory =
        new HttpComponentsClientHttpRequestFactory();

    HttpClientBuilder clientBuilder = HttpClientBuilder.create();
    // configures the clientBuilder
    if (connectionConfig.getAuthToken() != null) {
      List<Header> defaultHeaders = Arrays.asList(
          new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + connectionConfig.getAuthToken()));
      clientBuilder.setDefaultHeaders(defaultHeaders);
    } else if (connectionConfig.getUsername() != null) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(new AuthScope(connectionConfig.getHost(), connectionConfig.getPort()),
          new UsernamePasswordCredentials(connectionConfig.getUsername(), connectionConfig.getPassword()));
      clientBuilder.setDefaultCredentialsProvider(credsProvider);
    }

    clientBuilder.setSSLContext(connectionConfig.getSslContext());
    clientBuilder.setSSLHostnameVerifier(connectionConfig.getHostnameVerifier());

    requestFactory.setHttpClient(clientBuilder.build());
    restTemplate.setRequestFactory(requestFactory);

    // configure our own ObjectMapper
    MappingJackson2HttpMessageConverter messageConverter =
        new MappingJackson2HttpMessageConverter();
    messageConverter.setPrettyPrint(false);
    // the client should use a mapper that would ignore unknown properties in case the server
    // is a newer version than the client
    messageConverter.setObjectMapper(GeodeJsonMapper.getMapperIgnoringUnknownProperties());
    restTemplate.getMessageConverters().removeIf(
        m -> m.getClass().getName().equals(MappingJackson2HttpMessageConverter.class.getName()));
    restTemplate.getMessageConverters().add(messageConverter);

    return new RestTemplateClusterManagementServiceTransport(this.restTemplate);
  }
}
