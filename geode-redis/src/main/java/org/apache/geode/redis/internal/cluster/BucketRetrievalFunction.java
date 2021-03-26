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

package org.apache.geode.redis.internal.cluster;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisKey;

public class BucketRetrievalFunction implements InternalFunction<Void> {

  public static final String ID = "REDIS_BUCKET_SLOT_FUNCTION";
  public static final Boolean CLUSTER_SLOTS_WITH_REPLICAS =
      Boolean.getBoolean("redis.cluster-slots-with-replicas");
  private final String hostAddress;
  private final int redisPort;

  private BucketRetrievalFunction(String address, int redisPort) {
    if (address == null || address.isEmpty() || address.equals("0.0.0.0")) {
      InetAddress localhost = null;
      try {
        localhost = LocalHostUtil.getLocalHost();
      } catch (Exception ignored) {
      }
      hostAddress = localhost == null ? "localhost" : localhost.getHostAddress();
    } else {
      hostAddress = address;
    }

    this.redisPort = redisPort;
  }

  public static void register(String address, int redisPort) {
    FunctionService.registerFunction(new BucketRetrievalFunction(address, redisPort));
  }

  @Override
  public void execute(FunctionContext<Void> context) {
    Region<RedisKey, ByteArrayWrapper> region =
        context.getCache().getRegion(RegionProvider.REDIS_DATA_REGION);

    String memberId =
        context.getCache().getDistributedSystem().getDistributedMember().getUniqueId();
    LocalDataSet localPrimary = (LocalDataSet) PartitionRegionHelper.getLocalPrimaryData(region);
    RegionAdvisor advisor = ((PartitionedRegion) region).getRegionAdvisor();
    Map<Integer, List<String>> bucketToSecondaries = new HashMap<>();

    for (Integer bucketId : localPrimary.getBucketSet()) {
      List<String> a = advisor.getBucketOwners(bucketId).stream()
          .map(InternalDistributedMember::getId)
          .filter(y -> !y.equals(memberId))
          .collect(Collectors.toList());

      if (CLUSTER_SLOTS_WITH_REPLICAS) {
        bucketToSecondaries.put(bucketId, a);
      }
    }

    MemberBuckets mb = new MemberBuckets(memberId, hostAddress, redisPort,
        localPrimary.getBucketSet(), bucketToSecondaries);
    context.getResultSender().lastResult(mb);
  }

  @Override
  public String getId() {
    return ID;
  }

  public static class MemberBuckets implements Serializable {
    private final String memberId;
    private final String hostAddress;
    private final int port;
    private final Set<Integer> primaryBucketIds;
    private final Map<Integer, List<String>> secondaryBucketMembers;

    public MemberBuckets(String memberId, String hostAddress, int port,
        Set<Integer> primaryBucketIds, Map<Integer, List<String>> secondaryBucketIds) {
      this.memberId = memberId;
      this.hostAddress = hostAddress;
      this.port = port;
      this.primaryBucketIds = primaryBucketIds;
      this.secondaryBucketMembers = secondaryBucketIds;
    }

    public String getMemberId() {
      return memberId;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public int getPort() {
      return port;
    }

    public Set<Integer> getPrimaryBucketIds() {
      return primaryBucketIds;
    }

    public Map<Integer, List<String>> getSecondaryBucketMembers() {
      return secondaryBucketMembers;
    }
  }
}
