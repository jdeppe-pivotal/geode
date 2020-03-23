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

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PoolUtils;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

import org.apache.geode.cache.TimeoutException;

/**
 * Locking mechanism to support Redis operations
 */
public class RedisLockService implements RedisLockServiceMBean {

  private static final int DEFAULT_TIMEOUT = 1000;
  private final int timeoutMS;
  private final Map<KeyHashIdentifier, Lock> keyTolocks = new ConcurrentHashMap<>();
  private final ObjectPool<Lock> lockPool;

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
    this.lockPool = initializeLockPool();
  }

  private ObjectPool<Lock> initializeLockPool() {
    try {
      GenericObjectPool<Lock> genericObjectPool =
          new GenericObjectPool<>(new RedisLockObjectPoolFactory());
      // genericObjectPool.setMinIdle(1);
      // genericObjectPool.setMaxIdle(5);
      // genericObjectPool.setMaxTotal(100);
      // genericObjectPool.setMaxWaitMillis(1000);
      return PoolUtils.synchronizedPool(genericObjectPool);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getLockCount() {
    return keyTolocks.size();
  }

  /**
   * Attempt to obtain a lock against a given key. The actual lock is wrapped in a returned {@link
   * AutoCloseable}. The lock can be released either by calling {@code close} on the returned
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
    Lock lock = null;

    Lock oldLock = keyTolocks.get(lockKey);
    synchronized (this) {

      if (oldLock != null) {
        lock = oldLock;
      } else {
        try {
          lock = lockPool.borrowObject();
          Lock lock1 = keyTolocks.putIfAbsent(lockKey, lock);
          if (lock1 != null) {
            throw new RuntimeException("Panic!!");
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (!lock.tryLock(timeoutMS, TimeUnit.MILLISECONDS)) {
      throw new TimeoutException("Couldn't get lock for " + lockKey.toString());
    }

    return new AutoCloseableLock(lockKey, lock, this);
  }

  public void unlock(KeyHashIdentifier key) {
    Lock lockToReturn = this.keyTolocks.remove(key);
    if (lockToReturn != null) {
      try {
        lockPool.returnObject(lockToReturn);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static class RedisLockObjectPoolFactory extends BasePooledObjectFactory<Lock> {

    @Override
    public Lock create() throws Exception {
      return new ReentrantLock();
    }

    @Override
    public PooledObject<Lock> wrap(Lock lock) {
      return new DefaultPooledObject<>(lock);
    }
  }
}
