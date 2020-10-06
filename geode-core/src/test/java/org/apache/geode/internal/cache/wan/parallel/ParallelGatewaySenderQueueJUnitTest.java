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
package org.apache.geode.internal.cache.wan.parallel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.AbstractBucketRegionQueue;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue.MetaRegionFactory;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue.ParallelGatewaySenderQueueMetaRegion;

public class ParallelGatewaySenderQueueJUnitTest {

  private ParallelGatewaySenderQueue queue;
  private MetaRegionFactory metaRegionFactory;
  private GemFireCacheImpl cache;
  private AbstractGatewaySender sender;

  @Before
  public void createParallelGatewaySenderQueue() {
    cache = mock(GemFireCacheImpl.class);
    sender = mock(AbstractGatewaySender.class);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(sender.getCancelCriterion()).thenReturn(cancelCriterion);
    when(sender.getCache()).thenReturn(cache);
    when(sender.getMaximumQueueMemory()).thenReturn(100);
    when(sender.getLifeCycleLock()).thenReturn(new ReentrantReadWriteLock());
    when(sender.getId()).thenReturn("");
    metaRegionFactory = mock(MetaRegionFactory.class);
    queue = new ParallelGatewaySenderQueue(sender, Collections.emptySet(), 0, 1, metaRegionFactory);
  }

  @Test
  public void whenReplicatedDataRegionNotReadyShouldNotThrowException() throws Exception {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.makeHeapCopyIfOffHeap()).thenReturn(event);
    when(event.getRegion()).thenReturn(null);
    String regionPath = "/testRegion";
    when(event.getRegionPath()).thenReturn(regionPath);
    Mockito.doThrow(new IllegalStateException()).when(event).release();
    Queue backingList = new LinkedList();
    backingList.add(event);

    queue = spy(queue);
    doReturn(true).when(queue).isDREvent(any(), any());
    boolean putDone = queue.put(event);
    assertThat(putDone).isFalse();
  }

  @Test
  public void whenPartitionedDataRegionNotReadyShouldNotThrowException() throws Exception {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.makeHeapCopyIfOffHeap()).thenReturn(event);
    when(event.getRegion()).thenReturn(null);
    String regionPath = "/testRegion";
    when(event.getRegionPath()).thenReturn(regionPath);
    PartitionedRegion region = mock(PartitionedRegion.class);
    when(region.getFullPath()).thenReturn(regionPath);
    when(cache.getRegion(regionPath, true)).thenReturn(region);
    PartitionAttributes pa = mock(PartitionAttributes.class);
    when(region.getPartitionAttributes()).thenReturn(pa);
    when(pa.getColocatedWith()).thenReturn(null);

    Mockito.doThrow(new IllegalStateException()).when(event).release();
    Queue backingList = new LinkedList();
    backingList.add(event);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    queue = spy(queue);
    boolean putDone = queue.put(event);
    assertThat(putDone).isFalse();
  }

  private void testEnqueueToBrqAfterLockFailedInitialImageReadLock(boolean isTmpQueue)
      throws InterruptedException {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    String regionPath = "/userPR";
    when(event.getRegionPath()).thenReturn(regionPath);
    when(event.makeHeapCopyIfOffHeap()).thenReturn(event);
    when(event.getRegion()).thenReturn(null);
    when(event.getBucketId()).thenReturn(1);
    when(event.getShadowKey()).thenReturn(100L);
    when(sender.isPersistenceEnabled()).thenReturn(true);
    PartitionedRegionDataStore prds = mock(PartitionedRegionDataStore.class);
    PartitionedRegion prQ = mock(PartitionedRegion.class);
    AbstractBucketRegionQueue brq = mock(AbstractBucketRegionQueue.class);
    ReentrantReadWriteLock initializationLock = mock(ReentrantReadWriteLock.class);
    ReentrantReadWriteLock.ReadLock readLock = mock(ReentrantReadWriteLock.ReadLock.class);
    when(initializationLock.readLock()).thenReturn(readLock);
    doNothing().when(readLock).lock();
    doNothing().when(readLock).unlock();
    doNothing().when(brq).unlockWhenRegionIsInitializing();
    when(brq.getInitializationLock()).thenReturn(initializationLock);
    when(brq.lockWhenRegionIsInitializing()).thenReturn(true);
    when(prQ.getDataStore()).thenReturn(prds);
    when(prQ.getCache()).thenReturn(cache);
    when(prQ.getBucketName(1)).thenReturn("_B__PARALLEL_GATEWAY_SENDER_QUEUE_1");
    when(prds.getLocalBucketById(1)).thenReturn(null);
    PartitionedRegion userPR = mock(PartitionedRegion.class);
    PartitionAttributes pa = mock(PartitionAttributes.class);
    when(userPR.getPartitionAttributes()).thenReturn(pa);
    when(pa.getColocatedWith()).thenReturn(null);
    when(userPR.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_PARTITION);
    when(userPR.getFullPath()).thenReturn(regionPath);
    when(cache.getRegion("_PARALLEL_GATEWAY_SENDER_QUEUE")).thenReturn(prQ);
    when(cache.getRegion(regionPath, true)).thenReturn(userPR);
    when(prQ.getColocatedWithRegion()).thenReturn(userPR);
    RegionAdvisor ra = mock(RegionAdvisor.class);
    BucketAdvisor ba = mock(BucketAdvisor.class);
    when(userPR.getRegionAdvisor()).thenReturn(ra);
    when(ra.getBucketAdvisor(1)).thenReturn(ba);
    when(ba.getShadowBucketDestroyed()).thenReturn(false);

    prepareBrq(brq, isTmpQueue);

    Mockito.doThrow(new IllegalStateException()).when(event).release();
    Queue backingList = new LinkedList();
    backingList.add(event);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    InOrder inOrder = inOrder(brq, readLock);
    queue = spy(queue);
    queue.addShadowPartitionedRegionForUserPR(userPR);
    doNothing().when(queue).putIntoBucketRegionQueue(eq(brq), any(), eq(event));
    boolean putDone = queue.put(event);
    assertThat(putDone).isTrue();
    inOrder.verify(brq).lockWhenRegionIsInitializing();
    inOrder.verify(readLock).lock();
    inOrder.verify(readLock).unlock();
    inOrder.verify(brq).unlockWhenRegionIsInitializing();
  }

  private void prepareBrq(AbstractBucketRegionQueue brq, boolean isTmpQueue) {
    if (isTmpQueue) {
      when(cache.getRegionByPath("/__PR/_B__PARALLEL_GATEWAY_SENDER_QUEUE_1"))
          .thenReturn(null).thenReturn(brq);
    } else {
      when(cache.getRegionByPath("/__PR/_B__PARALLEL_GATEWAY_SENDER_QUEUE_1"))
          .thenReturn(brq);
    }
  }

  @Test
  public void enqueueToInitializingBrqShouldLockFailedInitialImageReadLock() throws Exception {
    testEnqueueToBrqAfterLockFailedInitialImageReadLock(false);
  }

  @Test
  public void enqueueToTmpQueueShouldLockFailedInitialImageReadLock() throws Exception {
    testEnqueueToBrqAfterLockFailedInitialImageReadLock(true);
  }

  @Test
  public void whenEventReleaseFromOffHeapFailsExceptionShouldNotBeThrownToAckReaderThread()
      throws Exception {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.makeHeapCopyIfOffHeap()).thenReturn(event);
    Mockito.doThrow(new IllegalStateException()).when(event).release();
    Queue backingList = new LinkedList();
    backingList.add(event);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List peeked = queue.peek(1, 1000);
    assertEquals(1, peeked.size());
    queue.remove();
  }

  @Test
  public void whenGatewayEventUnableToResolveFromOffHeapTheStatForNotQueuedConflatedShouldBeIncremented()
      throws Exception {
    GatewaySenderStats stats = mockGatewaySenderStats();

    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.makeHeapCopyIfOffHeap()).thenReturn(null);
    GatewaySenderEventImpl eventResolvesFromOffHeap = mock(GatewaySenderEventImpl.class);
    when(eventResolvesFromOffHeap.makeHeapCopyIfOffHeap()).thenReturn(eventResolvesFromOffHeap);
    Queue backingList = new LinkedList();
    backingList.add(event);
    backingList.add(eventResolvesFromOffHeap);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List peeked = queue.peek(1, 1000);
    assertEquals(1, peeked.size());
    verify(stats, times(1)).incEventsNotQueuedConflated();
  }

  private GatewaySenderStats mockGatewaySenderStats() {
    GatewaySenderStats stats = mock(GatewaySenderStats.class);
    when(sender.getStatistics()).thenReturn(stats);
    return stats;
  }

  @Test
  public void whenNullPeekedEventFromBucketRegionQueueTheStatForNotQueuedConflatedShouldBeIncremented()
      throws Exception {
    GatewaySenderStats stats = mockGatewaySenderStats();

    GatewaySenderEventImpl eventResolvesFromOffHeap = mock(GatewaySenderEventImpl.class);
    when(eventResolvesFromOffHeap.makeHeapCopyIfOffHeap()).thenReturn(eventResolvesFromOffHeap);
    Queue backingList = new LinkedList();
    backingList.add(null);
    backingList.add(eventResolvesFromOffHeap);

    BucketRegionQueue bucketRegionQueue = mockBucketRegionQueue(backingList);

    TestableParallelGatewaySenderQueue queue = new TestableParallelGatewaySenderQueue(sender,
        Collections.emptySet(), 0, 1, metaRegionFactory);
    queue.setMockedAbstractBucketRegionQueue(bucketRegionQueue);

    List peeked = queue.peek(1, 1000);
    assertEquals(1, peeked.size());
    verify(stats, times(1)).incEventsNotQueuedConflated();
  }

  @Test
  public void testLocalSize() throws Exception {
    ParallelGatewaySenderQueueMetaRegion mockMetaRegion =
        mock(ParallelGatewaySenderQueueMetaRegion.class);
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(mockMetaRegion.getDataStore()).thenReturn(dataStore);
    when(dataStore.getSizeOfLocalPrimaryBuckets()).thenReturn(3);
    when(metaRegionFactory.newMetataRegion(any(), any(), any(), any())).thenReturn(mockMetaRegion);
    when(cache.createVMRegion(any(), any(), any())).thenReturn(mockMetaRegion);

    queue.addShadowPartitionedRegionForUserPR(mockPR("region1"));

    assertEquals(3, queue.localSize());
  }

  @Test
  public void isDREventReturnsTrueForDistributedRegionEvent() {
    String regionPath = "regionPath";
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.getRegionPath()).thenReturn(regionPath);
    DistributedRegion region = mock(DistributedRegion.class);
    when(cache.getRegion(regionPath)).thenReturn(region);
    ParallelGatewaySenderQueue queue = mock(ParallelGatewaySenderQueue.class);
    when(queue.isDREvent(cache, event)).thenCallRealMethod();

    assertThat(queue.isDREvent(cache, event)).isTrue();
  }

  @Test
  public void isDREventReturnsFalseForPartitionedRegionEvent() {
    String regionPath = "regionPath";
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.getRegionPath()).thenReturn(regionPath);
    PartitionedRegion region = mock(PartitionedRegion.class);
    when(cache.getRegion(regionPath)).thenReturn(region);
    ParallelGatewaySenderQueue queue = mock(ParallelGatewaySenderQueue.class);
    when(queue.isDREvent(cache, event)).thenCallRealMethod();

    assertThat(queue.isDREvent(cache, event)).isFalse();
  }

  private PartitionedRegion mockPR(String name) {
    PartitionedRegion region = mock(PartitionedRegion.class);
    when(region.getFullPath()).thenReturn(name);
    when(region.getPartitionAttributes()).thenReturn(new PartitionAttributesFactory<>().create());
    when(region.getTotalNumberOfBuckets()).thenReturn(113);
    when(region.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    return region;
  }

  private BucketRegionQueue mockBucketRegionQueue(final Queue backingList) {
    PartitionedRegion mockBucketRegion = mockPR("bucketRegion");
    // These next mocked return calls are for when peek is called. It ends up checking these on the
    // mocked pr region
    when(mockBucketRegion.getLocalMaxMemory()).thenReturn(100);
    when(mockBucketRegion.size()).thenReturn(backingList.size());

    BucketRegionQueue bucketRegionQueue = mock(BucketRegionQueue.class);
    when(bucketRegionQueue.getPartitionedRegion()).thenReturn(mockBucketRegion);
    when(bucketRegionQueue.peek()).thenAnswer((Answer) invocation -> backingList.poll());
    return bucketRegionQueue;
  }



  private class TestableParallelGatewaySenderQueue extends ParallelGatewaySenderQueue {

    private BucketRegionQueue mockedAbstractBucketRegionQueue;

    public TestableParallelGatewaySenderQueue(final AbstractGatewaySender sender,
        final Set<Region> userRegions, final int idx, final int nDispatcher) {
      super(sender, userRegions, idx, nDispatcher);
    }

    public TestableParallelGatewaySenderQueue(final AbstractGatewaySender sender,
        final Set<Region> userRegions, final int idx, final int nDispatcher,
        final MetaRegionFactory metaRegionFactory) {
      super(sender, userRegions, idx, nDispatcher, metaRegionFactory);
    }


    public void setMockedAbstractBucketRegionQueue(BucketRegionQueue mocked) {
      this.mockedAbstractBucketRegionQueue = mocked;
    }

    public AbstractBucketRegionQueue getBucketRegion(final PartitionedRegion prQ,
        final int bucketId) {
      return mockedAbstractBucketRegionQueue;
    }

    @Override
    public boolean areLocalBucketQueueRegionsPresent() {
      return true;
    }

    @Override
    protected PartitionedRegion getRandomShadowPR() {
      return mockedAbstractBucketRegionQueue.getPartitionedRegion();
    }

    @Override
    protected int getRandomPrimaryBucket(PartitionedRegion pr) {
      return 0;
    }

    @Override
    protected BucketRegionQueue getBucketRegionQueueByBucketId(PartitionedRegion prQ,
        int bucketId) {
      return mockedAbstractBucketRegionQueue;
    }

    // @Override
    // public int localSizeForProcessor() {
    // return 1;
    // }
  }

}
