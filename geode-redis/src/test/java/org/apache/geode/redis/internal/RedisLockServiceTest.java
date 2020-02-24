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


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.Test;

import org.apache.geode.cache.TimeoutException;

/**
 * Test cases for the Redis lock service
 */
public class RedisLockServiceTest {
  private static boolean testLockBool = false;
  private static boolean testUnlock = false;

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

    // test locks across threads
    Thread t1 = new Thread(() -> {
      try {
        AutoCloseableLock autoLock = lockService.lock(key1);

        while (true) {
          if (RedisLockServiceTest.testLockBool) {
            autoLock.close();
            break;
          }

          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
          }
        }
      } catch (Exception e) {
      }
    });

    // start thread with locking
    t1.start();
    Thread.sleep(1000);

    // test current thread cannot lock the same key
    assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> lockService.lock(key1));

    // test current thread cannot lock the same (via equality) key
    assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> lockService.lock(key2));

    // set flag for thread to unlock key
    RedisLockServiceTest.testLockBool = true;
    t1.join();

    // assert true you can now lock the service
    assertThat(lockService.lock(key1)).isNotNull();
    assertThat(lockService.lock(key2)).isNotNull();

    assertThat(lockService.getMapSize()).isEqualTo(1);
    // TODO: clean this up - either remove or fix if we switch to some other backing structure in
    // RedisLockServiceTest
    // Object key2 = 123;
    // Assert.assertTrue(lockService.lock(key2));
    //
    // // check weak reference support
    // key2 = null;
    // System.gc();
    //
    // // lock should be removed when not references to key
    // Assert.assertNull(lockService.getLock(123));
    //
    // // check that thread 1 has stopped
    // t1.join();
  }

  /**
   * Test unlock method
   */
  @Test
  public void testUnlock() throws Exception {
    RedisLockService lockService1 = new RedisLockService();
    RedisLockService lockService2 = new RedisLockService();

    ByteArrayWrapper key = new ByteArrayWrapper(new byte[] {2});
    // test locks across threads
    Thread t1 = new Thread(() -> {

      try {
        AutoCloseableLock autoLock = lockService1.lock(new ByteArrayWrapper(new byte[] {2}));

        while (true) {
          if (RedisLockServiceTest.testUnlock) {
            autoLock.close();
            break;
          }

          try {
            Thread.sleep(5);
          } catch (InterruptedException e) {
          }
        }
      } catch (Exception e) {
      }
    });

    // start thread with locking
    t1.start();
    Thread.sleep(5);

    assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> lockService1.lock(key));

    // test locks across services are different
    try (AutoCloseableLock lock = lockService2.lock(key)) {
    } // implicit close

    assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> lockService1.lock(key));

    // set flag for thread to unlock
    RedisLockServiceTest.testUnlock = true;
    Thread.sleep(20);

    assertThat(lockService1.lock(key)).isNotNull();
    t1.join();
  }

  @Test
  public void testGetLock() throws InterruptedException {
    RedisLockService lockService = new RedisLockService();

    ByteArrayWrapper obj = new ByteArrayWrapper(new byte[] {1});

    AutoCloseableLock autoLock = lockService.lock(obj);
    assertThat(lockService.getMapSize()).isEqualTo(1);

    autoLock.close();
    autoLock = null;
    obj = null;

    System.gc();
    System.runFinalization();

    // check lock removed
    assertThat(lockService.getMapSize()).isEqualTo(0);
  }

  @Test
  public void testGetLockTwice() throws InterruptedException {
    RedisLockService lockService = new RedisLockService();

    ByteArrayWrapper obj1 = new ByteArrayWrapper(new byte[] {77});
    AutoCloseableLock lock1 = lockService.lock(obj1);

    assertThat(lockService.getMapSize()).isEqualTo(1);

    ByteArrayWrapper obj2 = new ByteArrayWrapper(new byte[] {77});
    AutoCloseableLock lock2 = lockService.lock(obj2);

    assertThat(lockService.getMapSize()).isEqualTo(1);

    obj1 = null;
    lock1 = null;

    System.gc();
    System.runFinalization();

    // check lock removed
    assertThat(lockService.getMapSize()).isEqualTo(1);
  }

}
