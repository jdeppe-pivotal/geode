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

package org.apache.geode.management.configuration;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.geode.cache.configuration.GatewayReceiverConfig;

public class RuntimeGatewayReceiverConfig extends GatewayReceiverConfig {

  private List<MemberRuntimeInfo> members;

  public RuntimeGatewayReceiverConfig() {}

  public RuntimeGatewayReceiverConfig(GatewayReceiverConfig config) {
    super(config);
  }

  public List<MemberRuntimeInfo> getMembers() {
    return members;
  }

  public void setMembers(
      List<MemberRuntimeInfo> members) {
    this.members = members;
  }

  public static class MemberRuntimeInfo {
    private int port;

    private int senderCount;

    private List<String> sendersConnected;

    private String memberId;

    private String memberRef;

    public MemberRuntimeInfo() {}

    public MemberRuntimeInfo(int port, int senderCount,
        List<String> sendersConnected, String memberId, String memberRef) {
      this.port = port;
      this.senderCount = senderCount;
      this.sendersConnected = sendersConnected;
      this.memberId = memberId;
      this.memberRef = memberRef;
    }

    public String getMemberId() {
      return memberId;
    }

    public void setMemberId(String memberId) {
      this.memberId = memberId;
    }

    public String getMemberRef() {
      return memberRef;
    }

    public void setMemberRef(String memberRef) {
      this.memberRef = memberRef;
    }

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public int getSenderCount() {
      return senderCount;
    }

    public void setSenderCount(int senderCount) {
      this.senderCount = senderCount;
    }

    @JsonInclude
    public List<String> getSendersConnected() {
      return sendersConnected;
    }

    public void setSendersConnected(List<String> sendersConnected) {
      this.sendersConnected = sendersConnected;
    }
  }
}
