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

package org.apache.geode.redis.internal.data;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.delta.DeltaInfo;

public class RedisList extends AbstractRedisData {

  public static final NullRedisList NULL_REDIS_LIST = new NullRedisList();

  private LinkedList<ByteArrayWrapper> list;

  private long size;

  // for serialization
  public RedisList() {}

  RedisList(Collection<ByteArrayWrapper> elements) {
    if (elements instanceof LinkedList) {
      this.list = (LinkedList<ByteArrayWrapper>) elements;
    } else {
      this.list = new LinkedList<>(elements);
    }
  }

  @Override
  protected void applyDelta(DeltaInfo deltaInfo) {
  }

  @Override
  protected boolean removeFromRegion() {
    return false;
  }

  @Override
  public RedisDataType getType() {
    return RedisDataType.REDIS_LIST;
  }

  long lpush(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key, List<ByteArrayWrapper> elements) {
    list.addAll(0, elements);
    size += elements.size();

    region.put(key, this);

    return size;
  }

  ByteArrayWrapper lpop(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key) {
    ByteArrayWrapper element = list.pop();
    size--;

    region.put(key, this);

    return element;
  }
}
