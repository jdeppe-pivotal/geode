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

package org.apache.geode.management.internal.configuration.mutators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.configuration.RuntimeGatewayReceiverConfig;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.CliUtil;

public class GatewayReceiverConfigManager implements ConfigurationManager<GatewayReceiverConfig> {
  private final InternalCache cache;

  public GatewayReceiverConfigManager(InternalCache cache) {
    this.cache = cache;
  }

  SystemManagementService getManagementService() {
    return (SystemManagementService) ManagementService.getExistingManagementService(cache);
  }

  @Override
  public void add(GatewayReceiverConfig config, CacheConfig existing) {
    existing.setGatewayReceiver(config);
  }

  @Override
  public void update(GatewayReceiverConfig config, CacheConfig existing) {
    existing.setGatewayReceiver(config);
  }

  @Override
  public void delete(GatewayReceiverConfig config, CacheConfig existing) {
    existing.setGatewayReceiver(null);
  }

  @Override
  public List<RuntimeGatewayReceiverConfig> list(GatewayReceiverConfig filterConfig,
      CacheConfig existing, String group) {
    final GatewayReceiverConfig gatewayReceiver = existing.getGatewayReceiver();
    if (gatewayReceiver == null) {
      return Collections.emptyList();
    }

    if (!filterConfig.getConfigGroup().equals(group)) {
      return Collections.emptyList();
    }

    List<RuntimeGatewayReceiverConfig.MemberRuntimeInfo> memberResults = new ArrayList<>();
    RuntimeGatewayReceiverConfig runtimeGatewayReceiverConfig =
        new RuntimeGatewayReceiverConfig(gatewayReceiver);

    // Gather members for group
    Set<DistributedMember> members = getMembers(group);

    for (DistributedMember member : members) {
      GatewayReceiverMXBean receiverBean = getGatewayReceiverMXBean(member);
      if (receiverBean != null) {
        RuntimeGatewayReceiverConfig.MemberRuntimeInfo memberRuntimeInfo =
            new RuntimeGatewayReceiverConfig.MemberRuntimeInfo();
        memberRuntimeInfo.setPort(receiverBean.getPort());
        memberRuntimeInfo.setSenderCount(receiverBean.getClientConnectionCount());
        memberRuntimeInfo
            .setSendersConnected(Arrays.asList(receiverBean.getConnectedGatewaySenders()));
        memberRuntimeInfo.setMemberId(member.getId());

        memberResults.add(memberRuntimeInfo);
      }
    }
    runtimeGatewayReceiverConfig.setMembers(memberResults);

    return Collections.singletonList(runtimeGatewayReceiverConfig);
  }

  @VisibleForTesting
  GatewayReceiverMXBean getGatewayReceiverMXBean(final DistributedMember member) {
    return Optional.ofNullable(MBeanJMXAdapter.getGatewayReceiverMBeanName(member))
        .map(objectName -> getManagementService().getMBeanProxy(objectName,
            GatewayReceiverMXBean.class))
        .orElse(null);
  }

  @VisibleForTesting
  Set<DistributedMember> getMembers(final String group) {
    if ("cluster".equals(group)) {
      return CliUtil.findMembers(null, new String[] {}, cache);
    }
    return CliUtil.findMembers(new String[] {group}, new String[] {}, cache);
  }

  @Override
  public GatewayReceiverConfig get(String id, CacheConfig existing) {
    return existing.getGatewayReceiver();
  }
}
