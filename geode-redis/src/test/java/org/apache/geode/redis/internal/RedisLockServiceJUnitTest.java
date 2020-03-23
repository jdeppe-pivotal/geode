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


import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import org.apache.geode.cache.TimeoutException;

/**
 * Test cases for the Redis lock service
 */
public class RedisLockServiceJUnitTest {
  /**
   * Test lock method
   *
   * @throws Exception when an unknown error occurs
   */
  @Test
  public void testLock() throws Exception {
    RedisLockService lockService = new RedisLockService(1000);

    // test null handling
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> lockService.lock(null));

    ByteArrayWrapper key1 = new ByteArrayWrapper(new byte[] {97, 98, 99});
    ByteArrayWrapper key2 = new ByteArrayWrapper(new byte[] {97, 98, 99});
    CountDownLatch latch = new CountDownLatch(1);

    // test locks across threads
    Thread t1 = new Thread(() -> {
      try {
        AutoCloseableLock autoLock = lockService.lock(key1);

        latch.await();
        autoLock.close();
      } catch (Exception e) {
      }
    });

    // start thread with locking
    t1.start();
    await().until(() -> lockService.getLockCount() == 1);

    // test current thread cannot lock the same key
    assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> lockService.lock(key1));

    // test current thread cannot lock the same (via equality) key
    assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> lockService.lock(key2));

    // Release the thread holding the lock
    latch.countDown();
    t1.join();

    // assert true you can now lock the service
    assertThat(lockService.lock(key1)).isNotNull();
    assertThat(lockService.lock(key2)).isNotNull();

    assertThat(lockService.getLockCount()).isEqualTo(1);
  }

  /**
   * Test unlock method
   */
  @Test
  public void testUnlock() throws Exception {
    RedisLockService lockService1 = new RedisLockService();
    RedisLockService lockService2 = new RedisLockService();

    ByteArrayWrapper key = new ByteArrayWrapper(new byte[] {2});
    CountDownLatch latch = new CountDownLatch(1);
    // test locks across threads
    Thread t1 = new Thread(() -> {

      try {
        AutoCloseableLock autoLock = lockService1.lock(new ByteArrayWrapper(new byte[] {2}));
        latch.await();
        autoLock.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    // start thread with locking
    t1.start();
    await().until(() -> lockService1.getLockCount() == 1);

    assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> lockService1.lock(key));

    // test locks across services are different
    try (AutoCloseableLock lock = lockService2.lock(key)) {
    } // implicit close

    assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> lockService1.lock(key));

    latch.countDown();

    assertThat(lockService1.lock(key)).isNotNull();
    t1.join();
  }

  @Test
  public void testGetLock() throws InterruptedException {
    RedisLockService lockService = new RedisLockService();

    ByteArrayWrapper obj = new ByteArrayWrapper(new byte[] {1});

    AutoCloseableLock autoLock = lockService.lock(obj);
    assertThat(lockService.getLockCount()).isEqualTo(1);

    autoLock.close();
    autoLock = null;
    obj = null;

    System.gc();
    System.runFinalization();

    // check lock removed
    await().until(() -> lockService.getLockCount() == 0);
    assertThat(lockService.getLockCount()).isEqualTo(0);
  }

  @Test
  public void testGetLockTwice() throws InterruptedException {
    RedisLockService lockService = new RedisLockService();

    ByteArrayWrapper obj1 = new ByteArrayWrapper(new byte[] {77});
    AutoCloseableLock lock1 = lockService.lock(obj1);

    assertThat(lockService.getLockCount()).isEqualTo(1);

    ByteArrayWrapper obj2 = new ByteArrayWrapper(new byte[] {77});
    AutoCloseableLock lock2 = lockService.lock(obj2);

    assertThat(lockService.getLockCount()).isEqualTo(1);

    obj1 = null;
    lock1 = null;

    System.gc();
    System.runFinalization();

    // check lock removed
    await().until(() -> lockService.getLockCount() == 1);
    assertThat(lockService.getLockCount()).isEqualTo(1);
  }

  @Test
  public void lockingDoesNotCauseConcurrentModificationExceptions()
      throws InterruptedException {

    int ITERATIONS = 100_000;
    RedisLockService lockService = new RedisLockService();

    ExecutorService pool = Executors.newFixedThreadPool(64);

    testWithSameKey(ITERATIONS, lockService, pool);

    testWithDifferentKeys(ITERATIONS, lockService, pool);
  }

  private void testWithDifferentKeys(int ITERATIONS, RedisLockService lockService,
      ExecutorService pool) throws InterruptedException {
    List<Callable<Integer>> lockMaker = new LinkedList<>();
    for (int i = 0; i < ITERATIONS; i++) {
      lockMaker.add(createLockCallable(lockService, "key-" + i));
    }
    List<Future<Integer>> future2 = pool.invokeAll(lockMaker);
    assertThat(future2.stream()
        .mapToInt(this::safeFutureGet).sum()).isEqualTo(ITERATIONS);
  }

  private void testWithSameKey(int ITERATIONS, RedisLockService lockService, ExecutorService pool)
      throws InterruptedException {
    List<Callable<Integer>> callables = new LinkedList<>();
    ByteArrayWrapper key = new ByteArrayWrapper("key".getBytes());
    for (int i = 0; i < ITERATIONS; i++) {
      callables.add(() -> {
        lockService.lock(key).close();
        return 1;
      });
    }

    List<Future<Integer>> future1 = pool.invokeAll(callables);
    assertThat(future1.stream()
        .mapToInt(this::safeFutureGet).sum()).isEqualTo(ITERATIONS);
  }

  private Callable<Integer> createLockCallable(RedisLockService lockService, String value) {
    return () -> {
      lockService.lock(new ByteArrayWrapper((value).getBytes())).close();
      return 1;
    };
  }

  private int safeFutureGet(Future<Integer> future) {
    try {
      return future.get();
    } catch (Exception cause) {
      throw new RuntimeException("Lock task failed", cause);
    }
  }
}
