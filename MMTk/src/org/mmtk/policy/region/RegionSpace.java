/*
 *  This file is part of the Jikes RVM project (http://jikesrvm.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  See the COPYRIGHT.txt file distributed with this work for information
 *  regarding copyright ownership.
 */
package org.mmtk.policy.region;


import org.mmtk.plan.Plan;
import org.mmtk.plan.TraceLocal;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.plan.g1.G1;
import org.mmtk.plan.g1.G1NurseryTraceLocal;
import org.mmtk.plan.g1.PauseTimePredictor;
import org.mmtk.policy.Space;
import org.mmtk.utility.*;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.heap.*;
import org.mmtk.utility.heap.layout.HeapLayout;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.*;
import org.vmmagic.unboxed.*;

import static org.mmtk.utility.Constants.*;

/**
 * Each instance of this class corresponds to one immix <b>space</b>.
 * Each of the instance methods of this class may be called by any
 * thread (i.e. synchronization must be explicit in any instance or
 * class method).  This contrasts with the SquishLocal, where
 * instances correspond to *plan* instances and therefore to kernel
 * threads.  Thus unlike this class, synchronization is not necessary
 * in the instance methods of SquishLocal.
 *
 */
@Uninterruptible
public final class RegionSpace extends Space {
  /* local status bits */
  public static final int LOCAL_GC_BITS_REQUIRED = 2;
  public static final int GC_HEADER_WORDS_REQUIRED = 0;
  private static final Offset HEAD_REGION_OFFSET = VM.objectModel.getFieldOffset(RegionSpace.class, "headRegion", Address.class);

  @Entrypoint private Address headRegion = Address.zero();
  int nurseryRegions = 0;
  public int committedRegions = 0;
  private AddressArray allocRegions = AddressArray.create(3);
  private final Lock allocLock = VM.newLock("alloc-lock");

  @Inline
  public Address firstRegion() {
    return headRegion;
  }

  @Inline
  public int maxRegions() {
    float total = (float) (VM.activePlan.global().getTotalPages() >>> Region.LOG_PAGES_IN_REGION) * Region.MEMORY_RATIO;
    return (int) total;
  }

  @Inline
  public float nurseryRatio() {
    float nursery = (float) nurseryRegions;
    float total = (float) (VM.activePlan.global().getTotalPages() >>> Region.LOG_PAGES_IN_REGION) * Region.MEMORY_RATIO;
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(total != 0f);
    return nursery / total;
  }

  @Inline
  public float committedRatio() {
    float committed = (float) committedRegions;
    float total = (float) (VM.activePlan.global().getTotalPages() >>> Region.LOG_PAGES_IN_REGION) * Region.MEMORY_RATIO;
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(total != 0f);
    return committed / total;
  }

  public RegionSpace(String name) {
    super(name, true, false, true, VMRequest.discontiguous());
    pr = new FreeListPageResource(this, Region.METADATA_PAGES_PER_CHUNK);
    ((FreeListPageResource) pr).pageOffsetLogAlign = Region.LOG_PAGES_IN_REGION;
  }

  @Override
  @Inline
  public void growSpace(Address start, Extent bytes, boolean newChunk) {
    if (newChunk) {
      Address chunk = Conversions.chunkAlign(start.plus(bytes), true);
      HeapLayout.mmapper.ensureMapped(chunk, Region.METADATA_PAGES_PER_CHUNK);
      VM.memory.zero(false, chunk, Extent.fromIntZeroExtend(Region.METADATA_PAGES_PER_CHUNK << LOG_BYTES_IN_PAGE));
    }
  }

  @Inline
  public void prepare() {
    prepare(false);
  }

  @NoInline
  public void clearNextMarkTables() {
    for (Address region = headRegion; !region.isZero(); region = Region.getNext(region)) {
      MarkTable.clearNextTable(region);
    }
  }

  @NoInline
  public void shiftMarkTables() {
    for (Address region = headRegion; !region.isZero(); region = Region.getNext(region)) {
      MarkTable.shiftTables(region);
    }
  }

  @NoInline
  public void prepare(boolean nursery) {
    resetAllocRegions();
    // Update meta
    for (Address region = headRegion; !region.isZero(); region = Region.getNext(region)) {
      Region.set(region, Region.MD_LIVE_SIZE, 0);
      Region.updatePrevCursor(region);
    }
  }

  @NoInline
  public void clearRemSetCardsPointingToCollectionSet() {
    for (Address region = headRegion; !region.isZero(); region = Region.getNext(region)) {
      RemSet.clearCardsInCollectionSet(region);
    }
  }

  @NoInline
  public void release() {
    resetAllocRegions();
    // Release all regions
    Address slot = ObjectReference.fromObject(this).toAddress().plus(HEAD_REGION_OFFSET);
    while (true) {
      Address region = slot.loadAddress();
      if (region.isZero()) break;
      if (Region.getBool(region, Region.MD_RELOCATE)) {
        Address next = Region.getAddress(region, Region.MD_NEXT_REGION);
        releaseRegion(region);
        slot.store(next);
      } else {
        slot = Region.metaSlot(region, Region.MD_NEXT_REGION);
      }
    }
  }

  @Inline
  public int getPagesUsed() {
    return pr.reservedPages();
  }

  public Address acquireRegion(int generation) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(generation <= 2);
    // Uninterruptedly acquire pages
    Address region;
    {
      boolean allowPoll = VM.activePlan.isMutator() && Plan.isInitialized();
      int pagesReserved = pr.reservePages(Region.PAGES_IN_REGION);
      if (allowPoll && VM.activePlan.global().poll(false, this)) {
        pr.clearRequest(pagesReserved);
        return Address.zero(); // GC required, return failure
      }
      Address rtn = pr.getNewPages(pagesReserved, Region.PAGES_IN_REGION, zeroed);
      if (rtn.isZero()) {
        if (!allowPoll) VM.assertions.fail("Physical allocation failed when polling not allowed!");
        boolean gcPerformed = VM.activePlan.global().poll(true, this);
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(gcPerformed, "GC not performed when forced.");
        pr.clearRequest(pagesReserved);
        return Address.zero();
      }
      region = rtn;
    }
    // Initialize region
    {
      if (Region.VERBOSE_REGION_LIFETIME) {
        Log.write("Alloc ");
        Log.write(Region.getGenerationName(generation));
        Log.writeln(" region ", region);
      }
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(Region.isAligned(region));
      }
      // Increase counter
      if (generation != Region.OLD) nurseryRegions += 1;
      committedRegions += 1;
      // Initialize metadata
      Region.register(region, generation);
      // Add to freelist
      Region.set(region, Region.MD_NEXT_REGION, headRegion);
      headRegion = region;
    }
    return region;
  }

  @Inline
  public void releaseRegion(Address region) {
    if (Region.VERBOSE_REGION_LIFETIME) {
      Log.write("Release ");
      Log.write(Region.getGenerationName(region));
      Log.writeln(" region ", region);
    }
    // Decrease counter
    committedRegions -= 1;
    if (Region.getInt(region, Region.MD_GENERATION) != Region.OLD) nurseryRegions -= 1;
    // Region is already removed from freelist. See `RegionSpace#release()`
    // Clear metadata
    Region.unregister(region);
    // Release memory
    ((FreeListPageResource) pr).releasePages(region);
  }

  @Override
  public Address acquire(int pages) {
    VM.assertions.fail("Unreachable");
    return Address.zero();
  }

  @Override
  public void release(Address region) {
    VM.assertions.fail("Unreachable");
  }

  @Inline
  public void resetAllocRegions() {
    for (int i = 0; i < 3; i++) {
      allocRegions.set(i, Address.zero());
    }
  }

  @Inline
  @LogicallyUninterruptible
  public Address allocTLAB(int allocationKind, int tlabSize) {
    if (tlabSize < Region.BYTES_IN_REGION) {
      Address tlab = allocTLABFastOnce(allocationKind, tlabSize);
      if (!tlab.isZero()) {
        return tlab;
      }
    }
    // Slow path
    Address result = allocTLABSlow(allocationKind, tlabSize);
    if (result.isZero()) {
      VM.collection.blockForGC();
    }
    return result;
  }

  @Inline
  @NoBoundsCheck
  private Address allocTLABFastOnce(int allocationKind, int tlabSize) {
    Address allocRegion = allocRegions.get(allocationKind);
    if (allocRegion.isZero()) return Address.zero();
    return Region.allocate(allocRegion, tlabSize, true);
  }

  private Address allocTLABSlow(int generation, int tlabSize) {
    allocLock.acquire();
    // Try again
    if (tlabSize < Region.BYTES_IN_REGION) {
      Address tlab = allocTLABFastOnce(generation, tlabSize);
      if (!tlab.isZero()) {
        allocLock.release();
        return tlab;
      }
    }
    // Acquire new region
    Address region = acquireRegion(generation);
    if (region.isZero()) {
      allocLock.release();
      return Address.zero();
    }
    Address result = Region.allocate(region, tlabSize, false);
    if (tlabSize < Region.BYTES_IN_REGION) allocRegions.set(generation, region);
    allocLock.release();
    return result;
  }

  @Override
  public ObjectReference traceObject(TransitiveClosure trace, ObjectReference object) {
    VM.assertions.fail("unsupported interface");
    return null;
  }

  @Inline
  public ObjectReference traceMarkObject(TransitiveClosure trace, ObjectReference object) {
    if (MarkTable.testAndMarkNext(object)) {
      Address region = Region.of(object);
      Region.updateRegionAliveSize(region, object);
      trace.processNode(object);
    }
    return object;
  }

  @Uninterruptible
  static public abstract class EvacuationTimer {
    @Inline
    public abstract void updateObjectEvacuationTime(long size, long time);
  }

  @Uninterruptible
  static public abstract class EvacuationAccumulator {
    @Inline
    public abstract void updateObjectEvacuationTime(long size, long time);
  }

  @Inline
  public ObjectReference traceEvacuateObject(TraceLocal trace, ObjectReference object, int allocator, EvacuationAccumulator evacuationTimer) {
    if (Region.getBool(Region.of(object), Region.MD_RELOCATE)) {
      Word priorStatusWord = ForwardingWord.attemptToForward(object);
      if (ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
        ObjectReference newObject = ForwardingWord.spinAndGetForwardedObject(object, priorStatusWord);
        return newObject;
      } else {
        long time = VM.statistics.nanoTime();
        ObjectReference newObject = ForwardingWord.forwardObject(object, allocator);
        if (evacuationTimer != null) {
          evacuationTimer.updateObjectEvacuationTime(VM.objectModel.getSizeWhenCopied(newObject), VM.statistics.nanoTime() - time);
        }
        trace.processNode(newObject);
        return newObject;
      }
    } else {
      if (MarkTable.testAndMarkNext(object)) {
        trace.processNode(object);
      }
      return object;
    }
  }

  @Inline
  public ObjectReference traceEvacuateObjectInCSet(TraceLocal trace, ObjectReference object, int allocator) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Region.getBool(Region.of(object), Region.MD_RELOCATE));
    Word priorStatusWord = ForwardingWord.attemptToForward(object);
    if (ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
      ObjectReference newObject = ForwardingWord.spinAndGetForwardedObject(object, priorStatusWord);
      return newObject;
    } else {
      if (trace instanceof G1NurseryTraceLocal) {
        G1NurseryTraceLocal ntrace = (G1NurseryTraceLocal) trace;
        ntrace.copyBytes += VM.objectModel.getSizeWhenCopied(object);
      }
      ObjectReference newObject = ForwardingWord.forwardObject(object, allocator);
      trace.processNode(newObject);
      return newObject;
    }
  }

  @Inline
  public static boolean isCrossRegionRef(ObjectReference src, Address slot, ObjectReference obj) {
    if (obj.isNull()) return false;
    Word x = slot.toWord();
    Word y = VM.objectModel.refToAddress(obj).toWord();
    return !x.xor(y).rshl(Region.LOG_BYTES_IN_REGION).isZero();
  }

  public void iterateToSpaceRemSetRoots(TraceLocal trace, int id, int numWorkers, boolean nursery) {
    long startTime = id == 0 ? VM.statistics.nanoTime() : 0;
    int cards = 0;
    VM.activePlan.collector().rendezvous();
    if (id == 0) atomicRegionIterator.reset();
    VM.activePlan.collector().rendezvous();
    Address region;
    while (!(region = atomicRegionIterator.next()).isZero()) {
      if (!Region.getBool(region, Region.MD_RELOCATE)) continue;
      cards += iterateRegionRemsetRoots(region, trace, nursery);
    }
    VM.activePlan.collector().rendezvous();
    if (id == 0) {
      G1.predictor.stat.totalRemSetTime += VM.statistics.nanoTime() - startTime;
    }
    G1.predictor.stat.totalRemSetCards.add(cards);
  }

  private static boolean remsetVisitorInNursery = false;

  private static final RemSet.Visitor remsetVisitor = new RemSet.Visitor<TraceLocal>() {
    @Uninterruptible @Inline public void visit(Address region, Address remset, Address card, TraceLocal context) {
      Card.linearScan(card, remsetRootsLinearScan, !remsetVisitorInNursery, context);
      RemSet.removeCard(region, card);
    }
  };

  private static final Card.LinearScan remsetRootsLinearScan = new Card.LinearScan<TraceLocal>() {
    @Uninterruptible @Inline public void scan(Address card, ObjectReference object, TraceLocal context) {
      context.processNode(object);
    }
  };

  private int iterateRegionRemsetRoots(Address region, TraceLocal trace, boolean nursery) {
    remsetVisitorInNursery = nursery;
    return RemSet.iterate(region, remsetVisitor, trace);
  }

  @Override
  public boolean isLive(ObjectReference object) {
    VM.assertions.fail("Unreachable");
    return false;
  }

  @Inline
  public boolean isLivePrev(ObjectReference object) {
//    if (ForwardingWord.isForwardedOrBeingForwarded(object)) return true;
    if (Region.allocatedWithinConurrentMarking(object)) return true;
    return MarkTable.isMarkedPrev(object);
  }

  @Inline
  public boolean isLiveNext(ObjectReference object) {
//    if (ForwardingWord.isForwardedOrBeingForwarded(object)) return true;
    if (Region.allocatedWithinConurrentMarking(object)) return true;
    return MarkTable.isMarkedNext(object);
  }

  @Inline
  public boolean contains(Address address) {
    return !address.isZero() && Space.isInSpace(descriptor, address) && Region.getBool(Region.of(address), Region.MD_ALLOCATED);
  }

  @Inline
  public boolean contains(ObjectReference ref) {
    return contains(VM.objectModel.refToAddress(ref));
  }


  private final AtomicRegionIterator atomicRegionIterator = new AtomicRegionIterator(this);

  @Uninterruptible
  private static class AtomicRegionIterator {
    private static final Offset REGION_OFFSET = VM.objectModel.getFieldOffset(AtomicRegionIterator.class, "region", Address.class);
    final RegionSpace space;
    @Entrypoint private Address region = Address.zero();

    AtomicRegionIterator(RegionSpace space) {
      this.space = space;
    }

    @Inline
    public void reset() {
      region = space.headRegion;
    }

    @Inline
    public Address next() {
      Address oldValue, newValue;
      Address slot = ObjectReference.fromObject(this).toAddress().plus(REGION_OFFSET);
      do {
        oldValue = slot.prepareAddress();
        if (oldValue.isZero()) return Address.zero();
        newValue = Region.getNext(oldValue);
      } while (!slot.attempt(oldValue, newValue));
      return oldValue;
    }
  }
}
