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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.configuration.RuntimeGatewayReceiverConfig;
import org.apache.geode.management.internal.beans.GatewayReceiverMBean;

public class GatewayReceiverConfigManagerTest {

  private InternalCache cache;

  @Before
  public void before() {
    cache = mock(InternalCache.class);
  }

  @Test
  public void listReturnsRuntimeConfig() {
    GatewayReceiverConfig filter = new GatewayReceiverConfig();
    CacheConfig existing = mock(CacheConfig.class);

    GatewayReceiverConfig receiverConfig = new GatewayReceiverConfig();
    receiverConfig.setGroup("group1");
    when(existing.getGatewayReceiver()).thenReturn(receiverConfig);

    GatewayReceiverConfigManager manager = new GatewayReceiverConfigManager(cache);
    GatewayReceiverConfigManager spyManager = spy(manager);
    GatewayReceiverMXBean gatewayReceiverMXBean = mock(GatewayReceiverMBean.class);
    DistributedMember member = mock(DistributedMember.class);
    Set<DistributedMember> members = Collections.singleton(member);

    when(gatewayReceiverMXBean.getPort()).thenReturn(5000);
    when(gatewayReceiverMXBean.getClientConnectionCount()).thenReturn(5);
    when(gatewayReceiverMXBean.getBindAddress()).thenReturn("127.0.0.1");
    when(gatewayReceiverMXBean.getConnectedGatewaySenders())
        .thenReturn(new String[] {"senderOne", "senderTwo"});

    when(member.getId()).thenReturn("member1");

    Mockito.doReturn(members).when(spyManager).getMembers(any());
    Mockito.doReturn(gatewayReceiverMXBean).when(spyManager).getGatewayReceiverMXBean(any());

    List<RuntimeGatewayReceiverConfig> result = spyManager.list(filter, existing, "cluster");

    assertThat(result).hasSize(1);
    assertThat(result.get(0).getId()).isEqualTo("group1");
    assertThat(result.get(0).getPort()).isEqualTo(5000);
    assertThat(result.get(0).getBindAddress()).isEqualTo("127.0.0.1");
    assertThat(result.get(0).getSendersConnected()).containsExactly("senderOne", "senderTwo");
    assertThat(result.get(0).getMemberId()).isEqualTo("member1");
    assertThat(result.get(0).getSenderCount()).isEqualTo(5);
  }

}
