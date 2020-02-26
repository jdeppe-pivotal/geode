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

package org.apache.geode.redis.internal.executor.set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Executor;

public class SetExecutorJUnitTest {

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSDiff() {
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    Command command = mock(Command.class);
    Executor sdiffExecutor = new SDiffExecutor();
    UnpooledByteBufAllocator byteBuf = new UnpooledByteBufAllocator(false);
    Mockito.when(context.getByteBufAllocator()).thenReturn(byteBuf);

    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SDIFF".getBytes());
    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sdiffExecutor.executeCommand(command, context);

    verify(command).setResponse(argsErrorCaptor.capture());

    assertThat(argsErrorCaptor.getValue().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSDiffStore() {
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    Command command = mock(Command.class);
    Executor sDiffStoreExecutor = new SDiffStoreExecutor();
    UnpooledByteBufAllocator byteBuf = new UnpooledByteBufAllocator(false);
    Mockito.when(context.getByteBufAllocator()).thenReturn(byteBuf);

    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SDIFFSTORE".getBytes());
    commandsAsBytes.add("key1".getBytes());
    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sDiffStoreExecutor.executeCommand(command, context);

    verify(command).setResponse(argsErrorCaptor.capture());

    assertThat(argsErrorCaptor.getValue().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }
}
