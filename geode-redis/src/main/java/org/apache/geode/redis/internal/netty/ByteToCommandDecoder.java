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
package org.apache.geode.redis.internal.netty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.statistics.RedisStats;

/**
 * This is the first part of the channel pipeline for Netty. Here incoming bytes are read and a
 * created {@link Command} is sent down the pipeline. It is unfortunate that this class is not
 * {@link io.netty.channel.ChannelHandler.Sharable} because no state is kept in this class. State is
 * kept by {@link ByteToMessageDecoder}, it may be worthwhile to look at a different decoder setup
 * as to avoid allocating a decoder for every new connection.
 * <p>
 * The code flow of the protocol parsing may not be exactly Java like, but this is done very
 * intentionally. It was found that in cases where large Redis requests are sent that end up being
 * fragmented, throwing exceptions when the command could not be fully parsed took up an enormous
 * amount of cpu time. The simplicity of the Redis protocol allows us to just back out and wait for
 * more data, while exceptions are left to malformed requests which should never happen if using a
 * proper Redis client.
 */
public class ByteToCommandDecoder extends ByteToMessageDecoder {

  /**
   * Important note
   * <p>
   * Do not use '' <-- java primitive chars. Redis uses {@link Coder#CHARSET} encoding so we should
   * not risk java handling char to byte conversions, rather just hard code {@link Coder#CHARSET}
   * chars as bytes
   */

  private static final byte rID = 13; // '\r';
  private static final byte nID = 10; // '\n';
  private static final byte bulkStringID = 36; // '$';
  private static final byte arrayID = 42; // '*';
  private static final int MAX_BULK_STRING_LENGTH = 512 * 1024 * 1024; // 512 MB

  private final RedisStats redisStats;

  private static Map<Integer, RedisCommandType> commandTypeMap;

  static {
    commandTypeMap = new HashMap<>();

    for (RedisCommandType t : RedisCommandType.values()) {
      int hash = 1;
      byte[] name = t.toString().getBytes();
      for (int i = 0; i < name.length; i++) {
        hash = 31 * hash + name[i];
      }

      commandTypeMap.put(hash, t);
    }
  }

  public ByteToCommandDecoder(RedisStats redisStats) {
    this.redisStats = redisStats;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    Command c = null;
    long bytesRead = 0;
    do {
      int startReadIndex = in.readerIndex();
      c = parse(in);
      if (c == null) {
        in.readerIndex(startReadIndex);
        break;
      }
      bytesRead += in.readerIndex() - startReadIndex;
      out.add(c);
    } while (in.isReadable()); // Try to take advantage of pipelining if it is being used
    redisStats.incNetworkBytesRead(bytesRead);
  }

  private Command parse(ByteBuf buffer) throws RedisCommandParserException {
    if (buffer == null) {
      throw new NullPointerException();
    }
    if (!buffer.isReadable()) {
      return null;
    }

    byte firstB = buffer.readByte();
    if (firstB != arrayID) {
      throw new RedisCommandParserException(
          "Expected: " + (char) arrayID + " Actual: " + (char) firstB);
    }

    return parseArrayToCommand(buffer);
  }

  private Command parseArrayToCommand(ByteBuf buffer)
      throws RedisCommandParserException {
    byte currentChar;
    int arrayLength = parseCurrentNumber(buffer);
    if (arrayLength == Integer.MIN_VALUE || !parseRN(buffer)) {
      return null;
    }
    if (arrayLength < 0 || arrayLength > 1000000000) {
      throw new RedisCommandParserException("invalid multibulk length");
    }

    currentChar = buffer.readByte();
    if (currentChar != bulkStringID) {
      throw new RedisCommandParserException(
          "expected: \'$\', got \'" + (char) currentChar + "\'");
    }

    RedisCommandType commandType = parseBulkStringAsCommand(buffer);
    if (commandType == null) {
      return null;
    }

    arrayLength--;
    List<byte[]> commandElems = new ArrayList<>(arrayLength);

    for (int i = 0; i < arrayLength; i++) {
      if (!buffer.isReadable()) {
        return null;
      }
      currentChar = buffer.readByte();
      if (currentChar == bulkStringID) {
        byte[] newBulkString = parseBulkString(buffer);
        if (newBulkString == null) {
          return null;
        }
        commandElems.add(newBulkString);
      } else {
        throw new RedisCommandParserException(
            "expected: \'$\', got \'" + (char) currentChar + "\'");
      }
    }

    return new Command(commandType, commandElems);
  }

  /**
   * Helper method to parse a bulk string when one is seen
   *
   * @param buffer Buffer to read from
   * @return byte[] representation of the Bulk String read
   * @throws RedisCommandParserException Thrown when there is illegal syntax
   */
  private byte[] parseBulkString(ByteBuf buffer) throws RedisCommandParserException {
    int bulkStringLength = parseCurrentNumber(buffer);
    if (bulkStringLength == Integer.MIN_VALUE) {
      return null;
    }
    if (bulkStringLength > MAX_BULK_STRING_LENGTH) {
      throw new RedisCommandParserException(
          "invalid bulk length, cannot exceed max length of " + MAX_BULK_STRING_LENGTH);
    }
    if (!parseRN(buffer)) {
      return null;
    }

    if (!buffer.isReadable(bulkStringLength)) {
      return null;
    }
    byte[] bulkString = new byte[bulkStringLength];
    buffer.readBytes(bulkString);

    if (!parseRN(buffer)) {
      return null;
    }

    return bulkString;
  }

  private RedisCommandType parseBulkStringAsCommand(ByteBuf buffer)
      throws RedisCommandParserException {
    int bulkStringLength = parseCurrentNumber(buffer);
    if (bulkStringLength == Integer.MIN_VALUE) {
      return null;
    }
    if (bulkStringLength > MAX_BULK_STRING_LENGTH) {
      throw new RedisCommandParserException(
          "invalid bulk length, cannot exceed max length of " + MAX_BULK_STRING_LENGTH);
    }
    if (!parseRN(buffer)) {
      return null;
    }

    if (!buffer.isReadable(bulkStringLength)) {
      return null;
    }

    int hash = 1;
    for (int i = 0; i < bulkStringLength; i++) {
      hash = 31 * hash + (buffer.readByte() & 0B11011111);
    }

    if (!parseRN(buffer)) {
      return null;
    }

    return commandTypeMap.getOrDefault(hash, RedisCommandType.UNKNOWN);
  }

  private RedisCommandType lookupCommandType(ByteBuf buf) {
    int hash = 1;
    int endIndex = buf.readerIndex() + buf.readableBytes();
    for (int i = buf.readerIndex(); i < endIndex; i++) {
      hash = 31 * hash + (buf.getByte(i) & 0B11011111);
    }

    return commandTypeMap.get(hash);
  }

  /**
   * Helper method to parse the number at the beginning of the buffer
   *
   * @param buffer Buffer to read
   * @return The number found at the beginning of the buffer
   */
  private int parseCurrentNumber(ByteBuf buffer) {
    int number = 0;
    int readerIndex = buffer.readerIndex();
    byte b = 0;
    while (true) {
      if (!buffer.isReadable()) {
        return Integer.MIN_VALUE;
      }
      b = buffer.readByte();
      if (Character.isDigit(b)) {
        number = number * 10 + (int) (b - '0');
        readerIndex++;
      } else {
        buffer.readerIndex(readerIndex);
        break;
      }
    }
    return number;
  }

  /**
   * Helper method that is called when the next characters are supposed to be "\r\n"
   *
   * @param buffer Buffer to read from
   * @throws RedisCommandParserException Thrown when the next two characters are not "\r\n"
   */
  private boolean parseRN(ByteBuf buffer) throws RedisCommandParserException {
    if (!buffer.isReadable(2)) {
      return false;
    }
    byte b = buffer.readByte();
    if (b != rID) {
      throw new RedisCommandParserException(
          "expected \'" + (char) rID + "\', got \'" + (char) b + "\'");
    }
    b = buffer.readByte();
    if (b != nID) {
      throw new RedisCommandParserException(
          "expected: \'" + (char) nID + "\', got \'" + (char) b + "\'");
    }
    return true;
  }

}
