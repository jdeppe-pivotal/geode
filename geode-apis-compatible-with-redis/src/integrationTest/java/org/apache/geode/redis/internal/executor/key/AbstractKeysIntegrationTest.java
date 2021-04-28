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

package org.apache.geode.redis.internal.executor.key;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractKeysIntegrationTest implements RedisPortSupplier {

  private JedisCluster jedisCluster;
  private Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedisCluster = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
    jedisCluster.close();
  }

  @Test
  public void errors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.KEYS, 1);
  }

  @Test
  public void givenSplat_withASCIIdata_returnsExpectedMatches() {
    jedis.set("string1", "v1");
    jedis.sadd("set1", "member1");
    jedis.hset("hash1", "key1", "field1");
    assertThat(jedis.keys("*")).containsExactlyInAnyOrder("string1", "set1", "hash1");
    assertThat(jedis.keys("s*")).containsExactlyInAnyOrder("string1", "set1");
    assertThat(jedis.keys("h*")).containsExactlyInAnyOrder("hash1");
    assertThat(jedis.keys("foo*")).isEmpty();
  }

  @Test
  public void forClusteredConnection_givenSplat_withASCIIdata_returnsExpectedMatches() {
    jedisCluster.set("{user100}string1", "v1");
    jedisCluster.sadd("{user100}set1", "member1");
    jedisCluster.hset("{user100}hash1", "key1", "field1");
    assertThat(jedisCluster.keys("{user100}*"))
        .containsExactlyInAnyOrder("{user100}string1", "{user100}set1", "{user100}hash1");
    assertThat(jedisCluster.keys("{user100}s*"))
        .containsExactlyInAnyOrder("{user100}string1", "{user100}set1");
    assertThat(jedisCluster.keys("{user100}h*"))
        .containsExactlyInAnyOrder("{user100}hash1");
    assertThat(jedisCluster.keys("{user100}foo*")).isEmpty();
  }

  @Test
  public void givenSplat_withBinaryData_returnsExpectedMatches() {
    byte[] stringKey =
        new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 't', 'r', 'i', 'n', 'g', '1'};
    byte[] value = new byte[] {'v', '1'};
    jedis.set(stringKey, value);
    byte[] setKey = new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 'e', 't', '1'};
    byte[] member = new byte[] {'m', '1'};
    jedis.sadd(setKey, member);
    byte[] hashKey = new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 'h', 'a', 's', 'h', '1'};
    byte[] key = new byte[] {'k', 'e', 'y', '1'};
    jedis.hset(hashKey, key, value);
    assertThat(jedis.exists(stringKey));
    assertThat(jedis.exists(setKey));
    assertThat(jedis.exists(hashKey));
    assertThat(jedis.keys(new byte[] {'*'})).containsExactlyInAnyOrder(stringKey, setKey, hashKey);
    assertThat(jedis.keys(new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', '*'}))
        .containsExactlyInAnyOrder(stringKey, setKey);
    assertThat(jedis.keys(new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 'h', '*'}))
        .containsExactlyInAnyOrder(hashKey);
    assertThat(jedis.keys(new byte[] {'f', '*'})).isEmpty();
  }

  @Test
  public void givenBinaryValue_withExactMatch_preservesBinaryData()
      throws UnsupportedEncodingException {
    String chinese_utf16 = "子";
    byte[] utf16encodedBytes = chinese_utf16.getBytes("UTF-16");
    byte[] stringKey =
        new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 't', 'r', 'i', 'n', 'g', '1'};
    byte[] allByteArray = new byte[utf16encodedBytes.length + stringKey.length];

    ByteBuffer buff = ByteBuffer.wrap(allByteArray);
    buff.put(utf16encodedBytes);
    buff.put(stringKey);
    byte[] combined = buff.array();

    jedis.set(combined, combined);
    assertThat(jedis.keys("*".getBytes())).containsExactlyInAnyOrder(combined);
  }

  @Test
  public void givenSplat_withCarriageReturnLineFeedAndTab_returnsExpectedMatches() {
    jedis.set(" foo bar ", "123");
    jedis.set(" foo\r\nbar\r\n ", "456");
    jedis.set(" \r\n\t\\x07\\x13 ", "789");

    assertThat(jedis.keys("*")).containsExactlyInAnyOrder(" \r\n\t\\x07\\x13 ", " foo\r\nbar\r\n ",
        " foo bar ");
  }

  @Test
  public void givenMalformedGlobPattern_returnsEmptySet() {
    assertThat(jedis.keys("[][]")).isEmpty();
  }
}
