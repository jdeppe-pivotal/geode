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
package org.apache.geode.redis;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class SetsJUnitTest {
  private static Jedis jedis;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static ThreePhraseGenerator generator = new ThreePhraseGenerator();
  private static int port = 6379;
  private static int ITERATIONS = 1000;

  @BeforeClass
  public static void setUp() throws IOException {
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GeodeRedisServer("localhost", port);

    server.start();
    jedis = new Jedis("localhost", port, 10000000);
  }

  @Test
  public void testSAddScard() {
    int elements = 10;
    Set<String> strings = new HashSet<String>();
    String key = generator.generate('x');
    for (int i = 0; i < elements; i++) {
      String elem = generator.generate('x');
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    Long response = jedis.sadd(key, stringArray);
    assertEquals(response, new Long(strings.size()));
    Long response2 = jedis.sadd(key, stringArray);
    assertThat(response2).isEqualTo(0L);

    assertEquals(jedis.scard(key), new Long(strings.size()));
  }

  @Test
  public void testConcurrentSAddScard() throws InterruptedException, ExecutionException {
    int elements = 1000;
    Jedis jedis2 = new Jedis("localhost", port, 10000000);
    Set<String> strings1 = new HashSet<String>();
    Set<String> strings2 = new HashSet<String>();
    String key = generator.generate('x');
    generateStrings(elements, strings1, 'y');
    generateStrings(elements, strings2, 'z');

    String[] stringArray1 = strings1.toArray(new String[strings1.size()]);
    String[] stringArray2 = strings2.toArray(new String[strings2.size()]);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Integer> callable1 = () -> doABunchOfSadds(key, stringArray1, jedis);
    Callable<Integer> callable2 = () -> doABunchOfSadds(key, stringArray2, jedis2);
    Future<Integer> future1 = pool.submit(callable1);
    Future<Integer> future2 = pool.submit(callable2);

    assertThat(future1.get()).isEqualTo(strings1.size());
    assertThat(future2.get()).isEqualTo(strings2.size());

    assertEquals(jedis.scard(key), new Long(strings1.size()+strings2.size()));

    pool.shutdown();
  }

  private void generateStrings(int elements, Set<String> strings, char separator) {
    for (int i = 0; i < elements; i++) {
      String elem = generator.generate(separator);
      strings.add(elem);
    }
  }

  private int doABunchOfSadds(String key, String[] strings,
                                Jedis jedis) {
    int successes = 0;

    for (int i = 0; i < strings.length; i++) {
      Long reply = jedis.sadd(key,strings[i]);
      if (reply == 1L) {
        successes++;
        Thread.yield();
      }
    }
    return successes;
  }

  @Test
  public void testSMembersIsMember() {
    int elements = 10;
    Set<String> strings = new HashSet<String>();
    String key = generator.generate('x');
    for (int i = 0; i < elements; i++) {
      String elem = generator.generate('x');
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.sadd(key, stringArray);

    Set<String> returnedSet = jedis.smembers(key);

    assertEquals(returnedSet, new HashSet<String>(strings));

    for (String entry : strings) {
      boolean exists = jedis.sismember(key, entry);
      assertTrue(exists);
    }
  }

  @Test
  public void testSMove() {
    String source = generator.generate('x');
    String dest = generator.generate('x');
    String test = generator.generate('x');
    int elements = 10;
    Set<String> strings = new HashSet<String>();
    generateStrings(elements, strings, 'x');
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.sadd(source, stringArray);

    long i = 1;
    for (String entry : strings) {
      long results = jedis.smove(source, dest, entry);
      assertTrue("results:" + results + " == 1", results == 1);
      assertTrue(jedis.sismember(dest, entry));

      results = jedis.scard(source);
      assertTrue(results + " == " + (strings.size() - i), results == strings.size() - i);
      assertTrue(jedis.scard(dest) == i);
      i++;
    }

    assertTrue(jedis.smove(test, dest, generator.generate('x')) == 0);
  }

  @Test
  public void testSDiffAndStore() {
    int numSets = 3;
    int elements = 10;
    String[] keys = new String[numSets];
    ArrayList<Set<String>> sets = new ArrayList<Set<String>>();
    for (int j = 0; j < numSets; j++) {
      keys[j] = generator.generate('x');
      Set<String> newSet = new HashSet<String>();
      for (int i = 0; i < elements; i++)
        newSet.add(generator.generate('x'));
      sets.add(newSet);
    }

    for (int i = 0; i < numSets; i++) {
      Set<String> s = sets.get(i);
      String[] stringArray = s.toArray(new String[s.size()]);
      jedis.sadd(keys[i], stringArray);
    }

    Set<String> result = sets.get(0);
    for (int i = 1; i < numSets; i++)
      result.removeAll(sets.get(i));

    assertEquals(result, jedis.sdiff(keys));

    String destination = generator.generate('x');

    jedis.sdiffstore(destination, keys);

    Set<String> destResult = jedis.smembers(destination);

    assertEquals(result, destResult);
  }

  @Test
  public void testSUnionAndStore() {
    int numSets = 3;
    int elements = 10;
    String[] keys = new String[numSets];
    ArrayList<Set<String>> sets = new ArrayList<Set<String>>();
    for (int j = 0; j < numSets; j++) {
      keys[j] = generator.generate('x');
      Set<String> newSet = new HashSet<String>();
      for (int i = 0; i < elements; i++)
        newSet.add(generator.generate('x'));
      sets.add(newSet);
    }

    for (int i = 0; i < numSets; i++) {
      Set<String> s = sets.get(i);
      String[] stringArray = s.toArray(new String[s.size()]);
      jedis.sadd(keys[i], stringArray);
    }

    Set<String> result = sets.get(0);
    for (int i = 1; i < numSets; i++)
      result.addAll(sets.get(i));

    assertEquals(result, jedis.sunion(keys));

    String destination = generator.generate('x');

    jedis.sunionstore(destination, keys);

    Set<String> destResult = jedis.smembers(destination);

    assertEquals(result, destResult);
  }

  @Test
  public void testSInterAndStore() {
    int numSets = 3;
    int elements = 10;
    String[] keys = new String[numSets];
    ArrayList<Set<String>> sets = new ArrayList<Set<String>>();
    for (int j = 0; j < numSets; j++) {
      keys[j] = generator.generate('x');
      Set<String> newSet = new HashSet<String>();
      for (int i = 0; i < elements; i++)
        newSet.add(generator.generate('x'));
      sets.add(newSet);
    }

    for (int i = 0; i < numSets; i++) {
      Set<String> s = sets.get(i);
      String[] stringArray = s.toArray(new String[s.size()]);
      jedis.sadd(keys[i], stringArray);
    }

    Set<String> result = sets.get(0);
    for (int i = 1; i < numSets; i++)
      result.retainAll(sets.get(i));

    assertEquals(result, jedis.sinter(keys));

    String destination = generator.generate('x');

    jedis.sinterstore(destination, keys);

    Set<String> destResult = jedis.smembers(destination);

    assertEquals(result, destResult);
  }

  @Test
  public void testConcurrentSadd() throws InterruptedException {
    Jedis jedis2 = new Jedis("localhost", port, 10000000);
    String key = generator.generate('x');

    Runnable runnable1 = () -> doABunchOfSadds(jedis, key, 1);
    Runnable runnable2 = () -> doABunchOfSadds(jedis2, key, 2);
    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.scard(key)).isEqualTo(ITERATIONS * 2);
  }

  @Test
  public void testConcurrentSmove() throws InterruptedException {
    Jedis jedis2 = new Jedis("localhost", port, 10000000);
    String source = generator.generate('x');
    String destination = generator.generate('x');

    List<String> fields = new ArrayList<>();
    for (int i = 0; i < ITERATIONS; i++) {
      String field = "value-" + i;
      jedis.sadd(source, field);
      fields.add(field);
    }

    Collections.shuffle(fields);
    BlockingQueue shuffledFields = new ArrayBlockingQueue(ITERATIONS, true, fields);

    Runnable runnable1 = () -> doABunchOfSMoves(jedis, source, destination, shuffledFields);
    Runnable runnable2 = () -> doABunchOfSMoves(jedis2, source, destination, shuffledFields);
    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.scard(source)).isEqualTo(0);
    assertThat(jedis.scard(destination)).isEqualTo(ITERATIONS);
  }

  private void doABunchOfSMoves(Jedis client, String source, String destination,
      BlockingQueue<String> shuffledFields) {
    String member;
    while (true) {
      try {
        member = shuffledFields.poll(100, TimeUnit.MILLISECONDS);
        if (member == null) {
          return;
        }
      } catch (InterruptedException e) {
        return;
      }

      client.smove(source, destination, member);
    }
  }

  private void doABunchOfSadds(Jedis client, String key, int index) {
    for (int i = 0; i < ITERATIONS; i++) {
      client.sadd(key, String.format("value-%d-%d", index, i));
    }
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    cache.close();
    server.shutdown();
  }
}
