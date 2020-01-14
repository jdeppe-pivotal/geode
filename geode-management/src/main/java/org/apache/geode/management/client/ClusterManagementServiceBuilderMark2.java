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

package org.apache.geode.management.client;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.ClusterManagementServiceConnectionConfig;
import org.apache.geode.management.api.ClusterManagementServiceTransportBuilder;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransportBuilder;
import org.apache.geode.management.internal.ClientClusterManagementService;

public class ClusterManagementServiceBuilderMark2 {

  private ClusterManagementServiceTransportBuilder transportBuilder;

  private ClusterManagementServiceConnectionConfig connectionConfig;

  public ClusterManagementService build() {
    if (transportBuilder == null && connectionConfig == null) {
      throw new IllegalStateException("Both transportBuilder and connectionConfig are null. Please configure with at least one of setTransportBuilder() or setConnectionConfig()");
    }

    if (transportBuilder == null) {
      transportBuilder = new RestTemplateClusterManagementServiceTransportBuilder();
    }

    if (connectionConfig == null) {
      return new ClientClusterManagementService(transportBuilder.build());
    }

    return new ClientClusterManagementService(transportBuilder.setConnectionConfig(connectionConfig).build());
  }

  public ClusterManagementServiceBuilderMark2 setTransport(
      ClusterManagementServiceTransportBuilder transportBuilder) {
    this.transportBuilder = transportBuilder;
    return this;
  }

  public ClusterManagementServiceBuilderMark2 setConnectionConfig(ClusterManagementServiceConnectionConfig connectionConfig) {
    this.connectionConfig = connectionConfig;
    return this;
  }
}