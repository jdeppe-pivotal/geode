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

import static org.apache.geode.management.configuration.Links.URI_VERSION;
import static org.apache.geode.management.internal.Constants.INCLUDE_CLASS_HEADER;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.internal.CompletableFutureProxy;
import org.apache.geode.management.runtime.OperationResult;
import org.apache.geode.management.runtime.RuntimeInfo;

public class RestTemplateClusterManagementServiceTransport
    implements ClusterManagementServiceTransport {

  private final RestTemplate restTemplate;

  private final ScheduledExecutorService longRunningStatusPollingThreadPool;

  public RestTemplateClusterManagementServiceTransport(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
    this.longRunningStatusPollingThreadPool = Executors.newScheduledThreadPool(1);
  }

  @Override
  public <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult submitMessage(
      T configMessage, CommandType command,
      Class<? extends ClusterManagementRealizationResult> responseType) {
    switch (command) {
      case CREATE:
        return create(configMessage, responseType);
      case DELETE:
        return delete(configMessage, responseType);
    }

    throw new IllegalArgumentException("Unable to process command " + command
        + ". Perhaps you need to use a different method in ClusterManagementServiceTransport.");
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementGetResult<T, R> submitMessageForGet(
      T config, Class<? extends ClusterManagementGetResult> responseType) {
    return restTemplate.exchange(getIdentityEndpoint(config), HttpMethod.GET, makeEntity(config),
        responseType)
        .getBody();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementListResult<T, R> submitMessageForList(
      T config, Class<? extends ClusterManagementListResult> responseType) {
    String endPoint = URI_VERSION + config.getLinks().getList();
    return restTemplate
        .exchange(endPoint + "/?id={id}&group={group}", HttpMethod.GET, makeEntity(config),
            responseType, config.getId(), config.getGroup())
        .getBody();
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementListOperationsResult<V> submitMessageForListOperation(
      A opType, Class<? extends ClusterManagementListOperationsResult> responseType) {
    final ClusterManagementListOperationsResult<V> result;

    // make the REST call to list in-progress operations
    result = assertSuccessful(restTemplate
        .exchange(URI_VERSION + opType.getEndpoint(), HttpMethod.GET,
            makeEntity(null), ClusterManagementListOperationsResult.class)
        .getBody());

    return new ClusterManagementListOperationsResult<>(
        result.getResult().stream().map(r -> reAnimate(r, opType.getEndpoint()))
            .collect(Collectors.toList()));
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<V> submitMessageForStart(
      A op) {
    final ClusterManagementOperationResult result;

    // make the REST call to start the operation
    result = assertSuccessful(restTemplate
        .exchange(URI_VERSION + op.getEndpoint(), HttpMethod.POST, makeEntity(op),
            ClusterManagementOperationResult.class)
        .getBody());

    // our restTemplate requires the url to be modified to start from "/v1"
    return reAnimate(result, op.getEndpoint());
  }

  @Override
  public boolean isConnected() {
    try {
      return restTemplate.getForEntity(URI_VERSION + "/ping", String.class)
          .getBody().equals("pong");
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void close() {
    longRunningStatusPollingThreadPool.shutdownNow();
  }

  private <V extends OperationResult> ClusterManagementOperationResult<V> reAnimate(
      ClusterManagementOperationResult<V> result, String endPoint) {
    String uri = URI_VERSION + endPoint + "/" + result.getOperationId();

    // complete the future by polling the check-status REST endpoint
    CompletableFuture<Date> futureOperationEnded = new CompletableFuture<>();
    CompletableFutureProxy<V> operationResult =
        new CompletableFutureProxy<>(restTemplate, uri, longRunningStatusPollingThreadPool,
            futureOperationEnded);

    return new ClusterManagementOperationResult<>(result, operationResult,
        result.getOperationStart(), futureOperationEnded, result.getOperator(),
        result.getOperationId());
  }


  private <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult create(T config,
      Class<? extends ClusterManagementRealizationResult> expectedResult) {
    String endPoint = URI_VERSION + config.getLinks().getList();
    // the response status code info is represented by the ClusterManagementResult.errorCode already
    return restTemplate.exchange(endPoint, HttpMethod.POST, makeEntity(config),
        expectedResult)
        .getBody();
  }

  private <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult delete(
      T config, Class<? extends ClusterManagementRealizationResult> expectedResult) {
    String uri = getIdentityEndpoint(config);
    return restTemplate.exchange(uri + "?group={group}",
        HttpMethod.DELETE,
        makeEntity(null),
        expectedResult,
        config.getGroup())
        .getBody();
  }

  public static <T> HttpEntity<T> makeEntity(T config) {
    HttpHeaders headers = new HttpHeaders();
    headers.add(INCLUDE_CLASS_HEADER, "true");
    return new HttpEntity<>(config, headers);
  }

  private String getIdentityEndpoint(AbstractConfiguration<?> config) {
    String uri = config.getLinks().getSelf();
    if (uri == null) {
      throw new IllegalArgumentException(
          "Unable to construct the URI with the current configuration.");
    }
    return URI_VERSION + uri;
  }

  private <T extends ClusterManagementResult> T assertSuccessful(T result) {
    if (result == null) {
      ClusterManagementResult somethingVeryBadHappened = new ClusterManagementResult(
          ClusterManagementResult.StatusCode.ERROR, "Unable to parse server response.");
      throw new ClusterManagementException(somethingVeryBadHappened);
    } else if (!result.isSuccessful()) {
      throw new ClusterManagementException(result);
    }
    return result;
  }
}