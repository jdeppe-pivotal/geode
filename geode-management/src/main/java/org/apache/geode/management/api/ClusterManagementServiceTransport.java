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

import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.OperationResult;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 * Interface which abstracts the transport between the CMS client and the endpoint. Currently only
 * an http implementation exists. However it does allow for different implementations; perhaps
 * something that doesn't use {@code RestTemplate}
 */
public interface ClusterManagementServiceTransport {

  <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult submitMessage(
      T configMessage, CommandType command,
      Class<? extends ClusterManagementRealizationResult> responseType);

  <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementListResult<T, R> submitMessageForList(
      T configMessage, Class<? extends ClusterManagementListResult> responseType);

  <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementGetResult<T, R> submitMessageForGet(
      T configMessage, Class<? extends ClusterManagementGetResult> responseType);

  <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementListOperationsResult<V> submitMessageForListOperation(
      A opType, Class<? extends ClusterManagementListOperationsResult> responseType);

  <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<V> submitMessageForStart(
      A op);

  boolean isConnected();

  void close();
}
