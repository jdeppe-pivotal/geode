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

package org.apache.geode.redis.internal;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.org.apache.hadoop.fs.GlobPattern;

class Subscriber {
  private static final Logger logger = LogService.getLogger();
  final Client client;
  String channel;
  GlobPattern pattern;

  private ExecutionHandlerContext context;

  private Subscriber(Client client, ExecutionHandlerContext context) {
    if (client == null) {
      throw new IllegalArgumentException("client cannot be null");
    }
    if (context == null) {
      throw new IllegalArgumentException("context cannot be null");
    }
    this.client = client;
    this.context = context;
  }

  public Subscriber(Client client, GlobPattern pattern, ExecutionHandlerContext context) {
    this(client, context);

    if (pattern == null) {
      throw new IllegalArgumentException("pattern cannot be null");
    }
    this.pattern = pattern;
  }

  public Subscriber(Client client, String channel, ExecutionHandlerContext context) {
    this(client, context);

    if (channel == null) {
      throw new IllegalArgumentException("channel cannot be null");
    }
    this.channel = channel;
  }

  public boolean isEqualTo(String channel, Client client) {
    return this.channel.equals(channel) && this.client.equals(client);
  }

  public boolean isEqualTo(GlobPattern pattern, Client client) {
    return this.pattern.equals(pattern) && this.client.equals(client);
  }

  public boolean publishMessage(String channel, String message) {
    ByteBuf messageByteBuffer = constructResponse(channel, message);
    if (messageByteBuffer == null) {
      return false;
    }

    return writeToChannelSynchronously(messageByteBuffer);
  }

  private ByteBuf constructResponse(String channel, String message) {
    ByteBuf messageByteBuffer;
    try {
      messageByteBuffer = Coder.getArrayResponse(context.getByteBufAllocator(),
          Arrays.asList("message", channel, message));
    } catch (CoderException e) {
      logger.warn("Unable to encode publish message", e);
      return null;
    }
    return messageByteBuffer;
  }

  /**
   * This method turns the response into a synchronous call. We want to
   * determine if the response, to the client, resulted in an error - for example if the client has
   * disconnected and the write fails. In such cases we need to be able to notify the caller.
   */
  private boolean writeToChannelSynchronously(ByteBuf messageByteBuffer) {
    ChannelFuture channelFuture = context.writeToChannel(messageByteBuffer);

    try {
      channelFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      return false;
    }
    return channelFuture.cause() == null;
  }
}
