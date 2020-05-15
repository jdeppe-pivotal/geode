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

package org.apache.geode.redis.internal.executor.hash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.geode.DataSerializer;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.EmptyRedisHash;

public class RedisHash implements RedisData {
  public static final RedisHash EMPTY = new EmptyRedisHash();
  private HashMap<ByteArrayWrapper, ByteArrayWrapper> hash;
  /**
   * When deltas are adds it will always contain an even number of field/value pairs.
   * When deltas are removes it will just contain field names.
   */
  private transient ArrayList<ByteArrayWrapper> deltas;
  // true if deltas contains adds; false if removes
  private transient boolean deltasAreAdds;


  public RedisHash(List<ByteArrayWrapper> fieldsToSet) {
    hash = new HashMap<>();
    Iterator<ByteArrayWrapper> iterator = fieldsToSet.iterator();
    while (iterator.hasNext()) {
      hash.put(iterator.next(), iterator.next());
    }
  }

  public RedisHash() {
    // for serialization
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashMap(hash, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    hash = DataSerializer.readHashMap(in);
  }

  @Override
  public boolean hasDelta() {
    return deltas != null;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    DataSerializer.writeBoolean(deltasAreAdds, out);
    DataSerializer.writeArrayList(deltas, out);
  }

  @Override
  public synchronized void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    boolean deltaAdds = DataSerializer.readBoolean(in);
    try {
      ArrayList<ByteArrayWrapper> deltas = DataSerializer.readArrayList(in);
      if (deltas != null) {
        Iterator<ByteArrayWrapper> iterator = deltas.iterator();
        while (iterator.hasNext()) {
          ByteArrayWrapper field = iterator.next();
          if (deltaAdds) {
            ByteArrayWrapper value = iterator.next();
            hash.put(field, value);
          } else {
            hash.remove(field);
          }
        }
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized int hset(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      List<ByteArrayWrapper> fieldsToSet, boolean nx) {
    int fieldsAdded = 0;
    Iterator<ByteArrayWrapper> iterator = fieldsToSet.iterator();
    while (iterator.hasNext()) {
      ByteArrayWrapper field = iterator.next();
      ByteArrayWrapper value = iterator.next();
      boolean added;
      if (nx) {
        added = hash.putIfAbsent(field, value) == null;
      } else {
        added = hash.put(field, value) == null;
      }
      if (added) {
        if (deltas == null) {
          deltas = new ArrayList<>();
        }
        deltas.add(field);
        deltas.add(value);
        fieldsAdded++;
      }
    }
    storeChanges(region, key, true);
    return fieldsAdded;
  }

  public synchronized int hdel(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      List<ByteArrayWrapper> fieldsToRemove) {
    int fieldsRemoved = 0;
    for (ByteArrayWrapper fieldToRemove : fieldsToRemove) {
      if (hash.remove(fieldToRemove) != null) {
        if (deltas == null) {
          deltas = new ArrayList<>();
        }
        deltas.add(fieldToRemove);
        fieldsRemoved++;
      }
    }
    storeChanges(region, key, false);
    return fieldsRemoved;
  }

  public synchronized Collection<ByteArrayWrapper> hgetall() {
    ArrayList<ByteArrayWrapper> result = new ArrayList<>();
    for (Map.Entry<ByteArrayWrapper, ByteArrayWrapper> entry : hash.entrySet()) {
      result.add(entry.getKey());
      result.add(entry.getValue());
    }
    return result;
  }

  public synchronized int hexists(ByteArrayWrapper field) {
    if (hash.containsKey(field)) {
      return 1;
    } else {
      return 0;
    }
  }

  public synchronized ByteArrayWrapper hget(ByteArrayWrapper field) {
    return hash.get(field);
  }

  public synchronized int hlen() {
    return hash.size();
  }

  public synchronized List<ByteArrayWrapper> hmget(List<ByteArrayWrapper> fields) {
    ArrayList<ByteArrayWrapper> results = new ArrayList<>(fields.size());
    for (ByteArrayWrapper field : fields) {
      results.add(hash.get(field));
    }
    return results;
  }

  public synchronized Collection<ByteArrayWrapper> hvals() {
    return new ArrayList<>(hash.values());
  }

  private void storeChanges(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      boolean doingAdds) {
    if (hasDelta()) {
      if (!doingAdds && hash.isEmpty()) {
        region.remove(key);
      } else {
        deltasAreAdds = doingAdds;
        try {
          region.put(key, this);
        } finally {
          deltas = null;
        }
      }
    }
  }

  // the following are needed because not all the hash commands have been converted to functions.

  public synchronized boolean isEmpty() {
    return hash.isEmpty();
  }

  public synchronized Collection<Map.Entry<ByteArrayWrapper, ByteArrayWrapper>> entries() {
    return new ArrayList<>(hash.entrySet());
  }

  public synchronized ByteArrayWrapper get(ByteArrayWrapper field) {
    return hash.get(field);
  }

  public synchronized void put(ByteArrayWrapper field, ByteArrayWrapper value) {
    hash.put(field, value);
  }

  public synchronized List<ByteArrayWrapper> keys() {
    return new ArrayList<>(hash.keySet());
  }

  @Override
  public RedisDataType getType() {
    return RedisDataType.REDIS_HASH;
  }
}
