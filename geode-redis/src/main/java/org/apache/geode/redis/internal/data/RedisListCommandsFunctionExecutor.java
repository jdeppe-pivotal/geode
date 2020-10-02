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

import java.util.List;

import org.apache.geode.redis.internal.executor.list.RedisListCommands;

public class RedisListCommandsFunctionExecutor extends RedisDataCommandsFunctionExecutor
    implements RedisListCommands {

  public RedisListCommandsFunctionExecutor(CommandHelper helper) {
    super(helper);
  }

  private RedisList getRedisList(ByteArrayWrapper key) {
    return helper.getRedisList(key);
  }

  @Override
  public Long lpush(ByteArrayWrapper key, List<ByteArrayWrapper> elements) {
    return stripedExecute(key, () -> getRedisList(key).lpush(getRegion(), key, elements));
  }

  @Override
  public ByteArrayWrapper lpop(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisList(key).lpop(getRegion(), key));
  }
}
