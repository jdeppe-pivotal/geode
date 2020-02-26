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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
    generateStrings(elements, strings, 'x');
    String[] stringArray = strings.toArray(new String[strings.size()]);

    Long response = jedis.sadd(key, stringArray);
    assertEquals(response, new Long(strings.size()));

    Long response2 = jedis.sadd(key, stringArray);
    assertThat(response2).isEqualTo(0L);

    assertEquals(jedis.scard(key), new Long(strings.size()));
  }

  @Test
  public void testConcurrentSAddScard_sameKeyPerClient()
      throws InterruptedException, ExecutionException {
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

    assertEquals(jedis.scard(key), new Long(strings1.size() + strings2.size()));

    pool.shutdown();
  }

  @Test
  public void testConcurrentSAddScard_differentKeyPerClient()
      throws InterruptedException, ExecutionException {
    int elements = 1000;
    Jedis jedis2 = new Jedis("localhost", port, 10000000);
    Set<String> strings = new HashSet<String>();
    String key1 = generator.generate('x');
    String key2 = generator.generate('y');
    generateStrings(elements, strings, 'y');

    String[] stringArray = strings.toArray(new String[strings.size()]);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Integer> callable1 = () -> doABunchOfSadds(key1, stringArray, jedis);
    Callable<Integer> callable2 = () -> doABunchOfSadds(key2, stringArray, jedis2);
    Future<Integer> future1 = pool.submit(callable1);
    Future<Integer> future2 = pool.submit(callable2);

    assertThat(future1.get()).isEqualTo(strings.size());
    assertThat(future2.get()).isEqualTo(strings.size());

    assertEquals(jedis.scard(key1), new Long(strings.size()));
    assertEquals(jedis.scard(key2), new Long(strings.size()));

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
      Long reply = jedis.sadd(key, strings[i]);
      if (reply == 1L) {
        successes++;
        Thread.yield();
      }
    }
    return successes;
  }

  @Test
  public void testSMembersSIsMember() {
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

    jedis.sadd(source, stringArray);
    String nonexistent = generator.generate('y');
    assertTrue(jedis.smove(source, dest, nonexistent) == 0);
    assertFalse(jedis.sismember(dest, nonexistent));
  }

  @Test
  public void testConcurrentSMove() throws ExecutionException, InterruptedException {
    String source = generator.generate('x');
    String dest = generator.generate('y');
    Jedis jedis2 = new Jedis("localhost", port, 10000000);
    int elements = 1000;
    Set<String> strings = new HashSet<String>();
    generateStrings(elements, strings, 'x');
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.sadd(source, stringArray);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Long> callable1 = () -> moveSetElements(source, dest, strings, jedis);
    Callable<Long> callable2 = () -> moveSetElements(source, dest, strings, jedis2);
    Future<Long> future1 = pool.submit(callable1);
    Future<Long> future2 = pool.submit(callable2);

    assertThat(future1.get() + future2.get()).isEqualTo(new Long(strings.size()));
    assertThat(jedis.smembers(dest).toArray()).containsExactlyInAnyOrder(strings.toArray());
    assertThat(jedis.scard(source)).isEqualTo(0L);
  }

  private long moveSetElements(String source, String dest, Set<String> strings,
      Jedis jedis) {
    long results = 0;
    for (String entry : strings) {
      results += jedis.smove(source, dest, entry);
      Thread.yield();
    }
    return results;
  }

  @Test
  public void testSDiff() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Set<String> result = jedis.sdiff("set1", "set2", "set3", "doesNotExist");
    String[] expected = new String[] {"pear", "plum", "orange"};
    assertThat(result).containsExactlyInAnyOrder(expected);

    Set<String> shouldNotChange = jedis.smembers("set1");
    assertThat(shouldNotChange).containsExactlyInAnyOrder(firstSet);

    Set<String> shouldBeEmpty = jedis.sdiff("doesNotExist", "set1", "set2", "set3");
    assertThat(shouldBeEmpty).isEmpty();

    Set<String> copySet = jedis.sdiff("set1");
    assertThat(copySet).containsExactlyInAnyOrder(firstSet);
  }

  @Test
  public void testSDiffStore() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Long resultSize = jedis.sdiffstore("result", "set1", "set2", "set3");
    Set<String> resultSet = jedis.smembers("result");

    String[] expected = new String[] {"pear", "plum", "orange"};
    assertThat(resultSize).isEqualTo(expected.length);
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Long otherResultSize = jedis.sdiffstore("set1", "set1", "result");
    Set<String> otherResultSet = jedis.smembers("set1");
    String[] otherExpected = new String[] {"apple", "peach"};
    assertThat(otherResultSize).isEqualTo(otherExpected.length);
    assertThat(otherResultSet).containsExactlyInAnyOrder(otherExpected);

    Long emptySetSize = jedis.sdiffstore("newEmpty", "nonexistent", "set2", "set3");
    Set<String> emptyResultSet = jedis.smembers("newEmpty");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    emptySetSize = jedis.sdiffstore("set1", "nonexistent", "set2", "set3");
    emptyResultSet = jedis.smembers("set1");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    Long copySetSize = jedis.sdiffstore("copySet", "set2");
    Set<String> copyResultSet = jedis.smembers("copySet");
    assertThat(copySetSize).isEqualTo(secondSet.length);
    assertThat(copyResultSet.toArray()).containsExactlyInAnyOrder((Object[]) secondSet);
  }

  @Test
  public void testConcurrentSDiffStore() throws InterruptedException {
    int ENTRIES = 100;
    int SUBSET_SIZE = 100;
    Jedis jedis2 = new Jedis("localhost", port, 10000000);

    Set<String> masterSet = new HashSet<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    List<Set<String>> otherSets = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      Set<String> oneSet = new HashSet<>();
      for (int j = 0; j < SUBSET_SIZE; j++) {
        oneSet.add("set-" + i + "-" + j);
      }
      otherSets.add(oneSet);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    for (int i = 0; i < ENTRIES; i++) {
      jedis.sadd("set-" + i, otherSets.get(i).toArray(new String[] {}));
      jedis.sadd("master", otherSets.get(i).toArray(new String[] {}));
    }

    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sdiffstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis2.sdiffstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.smembers("master").toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }

  @Test
  public void testSInter() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Set<String> resultSet = jedis.sinter("set1", "set2", "set3");

    String[] expected = new String[] {"peach"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Set<String> emptyResultSet = jedis.sinter("nonexistent", "set2", "set3");
    assertThat(emptyResultSet).isEmpty();

    jedis.sadd("newEmpty", "born2die");
    jedis.srem("newEmpty", "born2die");
    Set<String> otherEmptyResultSet = jedis.sinter("set2", "newEmpty");
    assertThat(otherEmptyResultSet).isEmpty();
  }

  @Test
  public void testSInterStore() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Long resultSize = jedis.sinterstore("result", "set1", "set2", "set3");
    Set<String> resultSet = jedis.smembers("result");

    String[] expected = new String[] {"peach"};
    assertThat(resultSize).isEqualTo(expected.length);
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Long otherResultSize = jedis.sinterstore("set1", "set1", "set2");
    Set<String> otherResultSet = jedis.smembers("set1");
    String[] otherExpected = new String[] {"apple", "peach"};
    assertThat(otherResultSize).isEqualTo(otherExpected.length);
    assertThat(otherResultSet).containsExactlyInAnyOrder(otherExpected);

    Long emptySetSize = jedis.sinterstore("newEmpty", "nonexistent", "set2", "set3");
    Set<String> emptyResultSet = jedis.smembers("newEmpty");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    emptySetSize = jedis.sinterstore("set1", "nonexistent", "set2", "set3");
    emptyResultSet = jedis.smembers("set1");
    assertThat(emptySetSize).isEqualTo(0L);
    assertThat(emptyResultSet).isEmpty();

    Long copySetSize = jedis.sinterstore("copySet", "set2", "newEmpty");
    Set<String> copyResultSet = jedis.smembers("copySet");
    assertThat(copySetSize).isEqualTo(0);
    assertThat(copyResultSet).isEmpty();
  }

  @Test
  public void testConcurrentSInterStore() throws InterruptedException {
    int ENTRIES = 100;
    int SUBSET_SIZE = 100;
    Jedis jedis2 = new Jedis("localhost", port, 10000000);

    Set<String> masterSet = new HashSet<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    List<Set<String>> otherSets = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      Set<String> oneSet = new HashSet<>();
      for (int j = 0; j < SUBSET_SIZE; j++) {
        oneSet.add("set-" + i + "-" + j);
      }
      otherSets.add(oneSet);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    for (int i = 0; i < ENTRIES; i++) {
      jedis.sadd("set-" + i, otherSets.get(i).toArray(new String[] {}));
      jedis.sadd("set-" + i, masterSet.toArray(new String[] {}));
    }

    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sinterstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis2.sinterstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.smembers("master").toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }

  @Test
  public void testSUnion() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Set<String> resultSet = jedis.sunion("set1", "set2");
    String[] expected =
        new String[] {"pear", "apple", "plum", "orange", "peach", "microsoft", "linux"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Set<String> otherResultSet = jedis.sunion("nonexistent", "set1");
    assertThat(otherResultSet).containsExactlyInAnyOrder(firstSet);

    jedis.sadd("newEmpty", "born2die");
    jedis.srem("newEmpty", "born2die");
    Set<String> yetAnotherResultSet = jedis.sunion("set2", "newEmpty");
    assertThat(yetAnotherResultSet).containsExactlyInAnyOrder(secondSet);
  }

  @Test
  public void testSUnionStore() {
    String[] firstSet = new String[] {"pear", "apple", "plum", "orange", "peach"};
    String[] secondSet = new String[] {"apple", "microsoft", "linux", "peach"};
    String[] thirdSet = new String[] {"luigi", "bowser", "peach", "mario"};
    jedis.sadd("set1", firstSet);
    jedis.sadd("set2", secondSet);
    jedis.sadd("set3", thirdSet);

    Long resultSize = jedis.sunionstore("result", "set1", "set2");
    assertThat(resultSize).isEqualTo(7);

    Set<String> resultSet = jedis.smembers("result");
    String[] expected =
        new String[] {"pear", "apple", "plum", "orange", "peach", "microsoft", "linux"};
    assertThat(resultSet).containsExactlyInAnyOrder(expected);

    Long notEmptyResultSize = jedis.sunionstore("notempty", "nonexistent", "set1");
    Set<String> notEmptyResultSet = jedis.smembers("notempty");
    assertThat(notEmptyResultSize).isEqualTo(firstSet.length);
    assertThat(notEmptyResultSet).containsExactlyInAnyOrder(firstSet);

    jedis.sadd("newEmpty", "born2die");
    jedis.srem("newEmpty", "born2die");
    Long newNotEmptySize = jedis.sunionstore("newNotEmpty", "set2", "newEmpty");
    Set<String> newNotEmptySet = jedis.smembers("newNotEmpty");
    assertThat(newNotEmptySize).isEqualTo(secondSet.length);
    assertThat(newNotEmptySet).containsExactlyInAnyOrder(secondSet);
  }

  @Test
  public void testConcurrentSUnionStore() throws InterruptedException {
    int ENTRIES = 100;
    int SUBSET_SIZE = 100;
    Jedis jedis2 = new Jedis("localhost", port, 10000000);

    Set<String> masterSet = new HashSet<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    List<Set<String>> otherSets = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      Set<String> oneSet = new HashSet<>();
      for (int j = 0; j < SUBSET_SIZE; j++) {
        oneSet.add("set-" + i + "-" + j);
      }
      otherSets.add(oneSet);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    for (int i = 0; i < ENTRIES; i++) {
      jedis.sadd("set-" + i, otherSets.get(i).toArray(new String[] {}));
    }

    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis.sunionstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        jedis2.sunionstore("master", "master", "set-" + i);
        Thread.yield();
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    otherSets.forEach(masterSet::addAll);

    assertThat(jedis.smembers("master").toArray()).containsExactlyInAnyOrder(masterSet.toArray());
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
