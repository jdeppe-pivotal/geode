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

import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.geode.cache.TimeoutException;

/**
 * Locking mechanism to support Redis operations
 */
public class RedisLockService implements RedisLockServiceMBean {

  private static final int DEFAULT_TIMEOUT = 1000;
  private final int timeoutMS;
  private final LoadingCache<KeyHashIdentifier, Lock> cache;

  /**
   * Construct with the default 1000ms timeout setting
   */
  public RedisLockService() {
    this(DEFAULT_TIMEOUT);
  }

  /**
   * Construct with the timeout setting
   *
   * @param timeoutMS the default timeout to wait for lock
   */
  public RedisLockService(int timeoutMS) {
    this.timeoutMS = timeoutMS;

    cache = CacheBuilder.newBuilder()
        .weakValues()
        .recordStats()
        .build(new CacheLoader<KeyHashIdentifier, Lock>() {
          @Override
          public Lock load(KeyHashIdentifier key) throws Exception {
            return new MarkerReentrantLock();
          }
        });
  }

  @Override
  public int getLockCount() {
    cache.cleanUp();
    return (int) (cache.stats().loadCount() - cache.stats().evictionCount());
  }

  /**
   * Attempt to obtain a lock against a given key. The actual lock is wrapped in a returned
   * {@link AutoCloseable}. The lock can be released either by calling {@code close} on the returned
   * object, or when the returned object becomes eligible for garbage collection (since the backing
   * data structure is based on a {@link WeakHashMap}).
   *
   * @param key the lock name/key
   * @return an {@link AutoCloseableLock}
   * @throws InterruptedException if the thread is interrupted
   * @throws TimeoutException if the lock cannot be acquired within the timeout period
   */
  public AutoCloseableLock lock(ByteArrayWrapper key) throws InterruptedException {
    if (key == null) {
      throw new IllegalArgumentException("key cannot be null");
    }

    KeyHashIdentifier lockKey = new KeyHashIdentifier(key.toBytes());

    Lock lock = cache.getUnchecked(lockKey);

    if (!lock.tryLock(timeoutMS, TimeUnit.MILLISECONDS)) {
      throw new TimeoutException("Couldn't get lock for " + lockKey.toString());
    }

    return new AutoCloseableLock(lockKey, lock);
  }

  public static class MarkerReentrantLock extends ReentrantLock {
  }
}
