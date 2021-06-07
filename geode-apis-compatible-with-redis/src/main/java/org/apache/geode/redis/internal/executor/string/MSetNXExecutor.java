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
package org.apache.geode.redis.internal.executor.string;


import java.util.List;

import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.executor.key.RedisKeyCommands;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class MSetNXExecutor extends AbstractExecutor {

  private static final int SET = 1;

  private static final int NOT_SET = 0;

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    List<byte[]> commandElems = command.getProcessedCommand();
    RedisStringCommands stringCommands = context.getStringCommands();
    RedisKeyCommands keyCommands = context.getKeyCommands();

    // TODO: make this atomic
    for (int i = 1; i < commandElems.size(); i += 2) {
      byte[] keyArray = commandElems.get(i);
      RedisKey key = new RedisKey(keyArray);
      if (keyCommands.exists(key)) {
        return RedisResponse.integer(NOT_SET);
      }
    }

    // none exist so now set them all
    for (int i = 1; i < commandElems.size(); i += 2) {
      byte[] keyArray = commandElems.get(i);
      RedisKey key = new RedisKey(keyArray);
      byte[] valueArray = commandElems.get(i + 1);
      stringCommands.set(key, valueArray, null);
    }

    return RedisResponse.integer(SET);
  }

}
