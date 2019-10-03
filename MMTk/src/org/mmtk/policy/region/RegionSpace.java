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
import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.plan.g1.CardRefinement;
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
  /** number of header bits we may use */
  private static final int AVAILABLE_LOCAL_BITS = 8 - HeaderByte.USED_GLOBAL_BITS;

  /* local status bits */
  public static final int LOCAL_GC_BITS_REQUIRED = 2;
  public static final int GLOBAL_GC_BITS_REQUIRED = 0;
  public static final int GC_HEADER_WORDS_REQUIRED = 0;
  private static boolean allocAsMarked = true;

  private Atomic.Int youngRegions = new Atomic.Int();
  private Atomic.Int committedRegions = new Atomic.Int();
  private final Lock allocLock = VM.newLock("alloc-lock");

  @Inline
  public int youngRegions() { return youngRegions.get(); }

  @Inline
  public int committedRegions() { return committedRegions.get(); }

  /**
   * The caller specifies the region of virtual memory to be used for
   * this space.  If this region conflicts with an existing space,
   * then the constructor will fail.
   *
   * @param name The name of this space (used when printing error messages etc)
   * @param vmRequest The virtual memory request
   */
  public RegionSpace(String name, VMRequest vmRequest) {
    this(name, true, VMRequest.discontiguous());
  }

  /**
   * The caller specifies the region of virtual memory to be used for
   * this space.  If this region conflicts with an existing space,
   * then the constructor will fail.
   *
   * @param name The name of this space (used when printing error messages etc)
   * @param zeroed if true, allocations return zeroed memory
   * @param vmRequest The virtual memory request
   */
  public RegionSpace(String name, boolean zeroed, VMRequest vmRequest) {
    super(name, true, false, zeroed, VMRequest.discontiguous());
    if (vmRequest.isDiscontiguous())
      pr = new FreeListPageResource(this, Region.METADATA_PAGES_PER_CHUNK);
    else
      pr = new FreeListPageResource(this, start, extent, Region.METADATA_PAGES_PER_CHUNK);
    ((FreeListPageResource) pr).pageOffsetLogAlign = Region.LOG_PAGES_IN_REGION;
  }

  public void makeAllocAsMarked() {
    this.allocAsMarked = true;
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
    Address r;
    regionIterator.reset();
    while (!(r = regionIterator.next()).isZero()) {
      MarkTable.clearNextTable(r);
    }
  }

  @NoInline
  public void shiftMarkTables() {
    Address r;
    regionIterator.reset();
    while (!(r = regionIterator.next()).isZero()) {
      MarkTable.shiftTables(r);
    }
  }

//  public void clearCSetMarkMap() {
//    Address r;
//    regionIterator.reset();
//    while (!(r = regionIterator.next()).isZero()) {
//      if (Region.getBool(r, Region.MD_RELOCATE))
//        Region.clearMarkBitMapForRegion(r);
//    }
//  }

  @NoInline
  public void prepare(boolean nursery) {
    // Update mark state
    // Clear marking data
    resetAllocRegions();
    Address r;
    regionIterator.reset();
    while (!(r = regionIterator.next()).isZero()) {
//      Region.clearMarkBitMapForRegion(r);
      Region.set(r, Region.MD_LIVE_SIZE, 0);
      Region.updatePrevCursor(r);
      // Set TAMS
      if (!nursery) {
//        Address cursor = Region.metaDataOf(r, Region.METADATA_CURSOR_OFFSET).loadAddress();
//        Log.write("region ", r);
//        Log.writeln(" cursor ", cursor);
//        if (VM.VERIFY_ASSERTIONS) {
//          VM.assertions._assert(cursor.GE(r));
//          if (!cursor.LE(r.plus(Region.BYTES_IN_REGION))) {
//            Log.write("Invalid cursor ", cursor);
//            Log.write( " region ", r);
//            Log.writeln( " end ", r.plus(Region.BYTES_IN_REGION));
//          }
//          VM.assertions._assert(cursor.LE(r.plus(Region.BYTES_IN_REGION)));
//        }
//        Region.metaDataOf(r, Region.METADATA_TAMS_OFFSET).store(cursor);
      }
    }
//    resetTLABs(1);
  }

  @NoInline
  public void clearRemSetCardsPointingToCollectionSet() {
    Address r;
    regionIterator.reset();
    while (!(r = regionIterator.next()).isZero()) {
      RemSet.clearCardsInCollectionSet(r);
    }
  }

  /**
   * A new collection increment has completed.  Release global resources.
   */
  @NoInline
  public void release() {
//    Address r;
//    regionIterator.reset();
//    while (!(r = regionIterator.next()).isZero()) {
//      Address remset = Region.getAddress(r, Region.MD_REMSET);
//      RemSet.clearCardsInCollectionSet(remset);
//    }
//       Set TAMS
//      Address slot = Region.metaDataOf(r, Region.METADATA_TAMS_OFFSET);
//      if (!nursery) {
//        Address cursor = Region.metaDataOf(r, Region.METADATA_CURSOR_OFFSET).loadAddress();
//        if (VM.VERIFY_ASSERTIONS) {
//          VM.assertions._assert(TAMS.GE(cursor));
//          VM.assertions._assert(TAMS.LT(cursor.plus(Region.BYTES_IN_REGION)));
//        }
//        Region.metaDataOf(r, Region.METADATA_TAMS_OFFSET).store(cursor);
//      }
//    }
    resetAllocRegions();
  }

  /**
   * Return the number of pages reserved for copying.
   */
  @Inline
  public int getCollectionReserve() {
    return 0;
  }

  @Inline
  public int getPagesUsed() {
    return pr.reservedPages() - getCollectionReserve();
  }

//  Lock tlabLock = VM.newLock("tlab-lock-m");

//  Atomic.Int tlabLock2 = new Atomic.Int();
//  int[] tlabLock3 = new int[] { 0 };
//  Lock tlabLockCollector = VM.newLock("tlab-lock-c");
//  Lock tlabCollectorLock = VM.newLock("tlab-lock-collector");
  AddressArray allocRegions = AddressArray.create(3);
//  AddressArray allocRegions = AddressArray.create(3);
//  AddressArray allocTLABs = AddressArray.create(3);
//  WordArray allocTLABIndex = WordArray.create(3);

  @Inline
  public void resetAllocRegions() {
    for (int i = 0; i < 3; i++) {
      allocRegions.set(i, Address.zero());
    }
  }

  @Inline
  public Address allocTLABFastOnce(int allocationKind, int tlabSize) {
    Address allocRegion = allocRegions.get(allocationKind);
    if (!allocRegion.isZero()) {
      return Region.allocate(allocRegion, tlabSize, true);
    } else {
      return Address.zero();
    }
  }

  public Address allocTLABSlow(int generation, int tlabSize) {
    allocLock.acquire();
    // Try again
    {
      Address tlab = allocTLABFastOnce(generation, tlabSize);
      if (!tlab.isZero()) {
        allocLock.release();
        return tlab;
      }
    }
    // Acquire new region
    Address region = acquireWithLock(Region.PAGES_IN_REGION);
    if (region.isZero()) {
      allocLock.release();
      return Address.zero();
    }
    if (generation != Region.OLD) youngRegions.add(1);
    committedRegions.add(1);
    insertRegion(region);
    Region.register(region, generation);
    Address result = Region.allocate(region, tlabSize, false);
    allocRegions.set(generation, region);
    allocLock.release();
    return result;
  }

  @Inline
  @LogicallyUninterruptible
  public Address allocTLAB(int allocationKind, int tlabSize) {
    Address tlab = allocTLABFastOnce(allocationKind, tlabSize);
    if (!tlab.isZero()) {
      return tlab;
    }
    // Slow path
    Address result = allocTLABSlow(allocationKind, tlabSize);
    if (result.isZero()) {
      VM.collection.blockForGC();
    }
    return result;
  }

  public Address acquireWithLock(int pages) {
    boolean allowPoll = VM.activePlan.isMutator() && Plan.isInitialized();
    /* Check page budget */
    int pagesReserved = pr.reservePages(pages);
    /* Poll, either fixing budget or requiring GC */
    if (allowPoll && VM.activePlan.global().poll(false, this)) {
      pr.clearRequest(pagesReserved);
      return Address.zero(); // GC required, return failure
    }
    /* Page budget is ok, try to acquire virtual memory */
    Address rtn = pr.getNewPages(pagesReserved, pages, zeroed);
    if (rtn.isZero()) {
      /* Failed, so force a GC */
      if (!allowPoll) VM.assertions.fail("Physical allocation failed when polling not allowed!");
      boolean gcPerformed = VM.activePlan.global().poll(true, this);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(gcPerformed, "GC not performed when forced.");
      pr.clearRequest(pagesReserved);
      return Address.zero();
    }
    return rtn;
  }

  /**
   * Release a region.  A region is free, so call the underlying page allocator
   * to release the associated storage.
   *
   * @param region The address of the Z Page to be released
   */
  @Override
  @NoInline
  public void release(Address region) {
    if (Region.verbose()) Log.writeln("Release region ", region);
    committedRegions.add(-1);
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(committedRegions.get() >= 0);

    if (Region.getInt(region, Region.MD_GENERATION) != Region.OLD) {
      youngRegions.add(-1);
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(youngRegions.get() >= 0);
    }

    Region.unregister(region);
//    if (Region.USE_CARDS) {
//      Region.Card.clearCardMetaForRegion(region);
//    }
    removeRegion(region);
    ((FreeListPageResource) pr).releasePages(region);
  }

  @Inline
  public void initializeHeader(ObjectReference object, int size) {
//    if (allocAsMarked) {
//      testAndMark(object);
//      if (HeaderByte.NEEDS_UNLOGGED_BIT) HeaderByte.markAsLogged(object);
//      Region.updateRegionAliveSize(Region.of(object), object);
//    } else {
//      VM.objectModel.writeAvailableByte(object, (byte) 0);
//    }
//    Region.updateRegionAliveSize(Region.of(object), object);
//    object.toAddress().store(Word.zero(), VM.objectModel.GC_HEADER_OFFSET());
  }

  @Inline
  public void postCopy(ObjectReference object, int size) {
    ForwardingWord.clearForwardingBits(object);
//    MarkTable.writeMarkState(object);
//    testAndMark(object);
//    if (HeaderByte.NEEDS_UNLOGGED_BIT) HeaderByte.markAsLogged(object);
//    object.toAddress().store(Word.zero(), VM.objectModel.GC_HEADER_OFFSET());
//    ForwardingWord.clearForwardingBits(object);
//    if (HeaderByte.NEEDS_UNLOGGED_BIT) HeaderByte.markAsLogged(object);
  }

  /**
   * Trace a reference to an object.  This interface is not supported by immix, since
   * we require the allocator to be identified except for the special case of the fast
   * trace.
   *
   * @param trace The trace performing the transitive closure
   * @param object The object to be traced.
   * @return null and fail.
   */
  @Override
  public ObjectReference traceObject(TransitiveClosure trace, ObjectReference object) {
    VM.assertions.fail("unsupported interface");
    return null;
  }

//  static final int[][][] markTable; // int[COLLECTORS][REGIONS][BITMAP]

  /**
   * Trace an object under a copying collection policy.
   * If the object is already copied, the copy is returned.
   * Otherwise, a copy is created and returned.
   * In either case, the object will be marked on return.
   *
   * @param trace The trace being conducted.
   * @param object The object to be forwarded.
   * @return The forwarded object.
   */
  @Inline
  public ObjectReference traceMarkObject(TransitiveClosure trace, ObjectReference object) {
//    ObjectReference rtn = object;

//    if (VM.VERIFY_ASSERTIONS) {
//      if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
//        VM.objectModel.dumpObject(object);
//      }
//      VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
//    }

    if (MarkTable.testAndMarkNext(object)) {
      if (HeaderByte.NEEDS_UNLOGGED_BIT) HeaderByte.attemptLog(object);
//      Log.writeln("Mark ", object);
      Address region = Region.of(object);
      Region.updateRegionAliveSize(region, object);
      trace.processNode(object);
//      return object;
    }
//    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(VM.debugging.validRef(object));
//      VM.assertions._assert(isLive(object));
//    }
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
  public ObjectReference traceEvacuateCSetObject(TraceLocal trace, ObjectReference object, int allocator, EvacuationAccumulator evacuationTimer) {
    if (Region.getBool(Region.of(object), Region.MD_RELOCATE)) {
      Word priorStatusWord = ForwardingWord.attemptToForward(object);

      if (ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
        return ForwardingWord.spinAndGetForwardedObject(object, priorStatusWord);
      } else {
        if (VM.VERIFY_ASSERTIONS) {
          VM.assertions._assert(VM.debugging.validRef(object));
        }
        long time = VM.statistics.nanoTime();
        ObjectReference newObject = ForwardingWord.forwardObject(object, allocator);
        evacuationTimer.updateObjectEvacuationTime(VM.objectModel.getSizeWhenCopied(newObject), VM.statistics.nanoTime() - time);
        trace.processNode(newObject);
        return newObject;
      }
    } else {
//      if (testAndMark(object))
//        trace.processNode(object);
      return object;
    }
  }

//  @Inline
//  public ObjectReference traceForwardCSetObject(TransitiveClosure trace, ObjectReference object) {
////    if (Region.relocationRequired(Region.of(object)))
//    if (!ForwardingWord.isForwarded(object)) {
////      if (VM.VERIFY_ASSERTIONS)
////        VM.assertions._assert(!Region.relocationRequired(Region.of(object)));
//      return object;
//    }
//    if (VM.VERIFY_ASSERTIONS)
//      VM.assertions._assert(Region.getBool(Region.of(object), Region.MD_RELOCATE));
////    Word status = VM.objectModel.readAvailableBitsWord(object);
//    ObjectReference ref = ForwardingWord.getForwardedObject(object);
//
//    if (MarkTable.testAndMark(ref)) trace.processNode(ref);
////    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(ref));
//    return ref;
////    ObjectReference newObject = ForwardingWord.getForwardedObject(object);
////    object = newObject.isNull() ? object : newObject;
////    return object;
//  }

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
//    let start_time = ::std::time::SystemTime::now();
    int regions = this.regions.length();
    int size = (regions + numWorkers - 1) / numWorkers;
    int start = size * id;
    int _limit = size * (id + 1);
    int limit = _limit > regions ? regions : _limit;
//    regionIterator
//    let mut cards = 0;
//
    for (int i = start; i < limit; i++) {
      Address region = this.regions.get(i);
      if (region.isZero()) continue;
      if (!Region.getBool(region, Region.MD_RELOCATE)) continue;
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!Region.getBool(region, Region.MD_RELOCATE));
      /* cards += */iterateRegionRemsetRoots(region, trace, nursery);
    }
//    let time = start_time.elapsed().unwrap().as_millis() as usize;
//    timer.report_remset_card_scanning_time(time, cards);
  }

  private static boolean remsetVisitorInNursery = false;
  private static final RemSet.Visitor remsetVisitor = new RemSet.Visitor<TraceLocal>() {
    @Uninterruptible @Inline public void visit(Address region, Address remset, Address card, TraceLocal context) {
      Card.linearScan(card, remsetRootsLinearScan, !remsetVisitorInNursery, context);
      RemSet.removeCard(region, card);
    }
  };
  private static final CardRefinement.LinearScan remsetRootsLinearScan = new CardRefinement.LinearScan<TraceLocal>() {
    @Uninterruptible @Inline public void scan(Address card, ObjectReference object, TraceLocal context) {
      context.processNode(object);
    }
  };

  private void iterateRegionRemsetRoots(Address region, TraceLocal trace, boolean nursery) {
    remsetVisitorInNursery = nursery;
    RemSet.iterate(region, remsetVisitor, trace);
  }

//  @Inline
//  public ObjectReference traceForwardObject(TransitiveClosure trace, ObjectReference object) {
//    if (Region.getBool(Region.of(object), Region.MD_RELOCATE)) {
////      Word status = VM.objectModel.readAvailableBitsWord(object);
//      object = ForwardingWord.getForwardedObject(object);
////
////      if (VM.VERIFY_ASSERTIONS) {
////        VM.assertions._assert(!object.isNull());
////      }
//    }
//
////    ObjectReference newObject = ForwardingWord.getForwardedObject(object);
////    if (VM.VERIFY_ASSERTIONS) {
////      if (Region.relocationRequired(Region.of(object))) {
////        if (newObject.isNull())
////          Log.writeln("Unforwarded object ", object);
////        VM.assertions._assert(!newObject.isNull());
////        VM.assertions._assert(newObject.toAddress().NE(object.toAddress()));
////      }
////    }
////    object = newObject.isNull() ? object : newObject;
////    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(object));
//    if (MarkTable.testAndMark(object)) {
////      HeaderByte.markAsLogged(object);
//      trace.processNode(object);
//    }
////    if (HeaderByte.isUnlogged(object)) {
////      VM.assertions.fail("Redirected object set is unlogged");
////    }
////    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(object));
//    return object;
//  }

  /**
   * Generic test of the liveness of an object
   *
   * @param object The object in question
   * @return {@code true} if this object is known to be live (i.e. it is marked)
   */
  @Override
  @Inline
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
    if (ForwardingWord.isForwardedOrBeingForwarded(object)) return true;
    if (Region.allocatedWithinConurrentMarking(object)) return true;
    return MarkTable.isMarkedNext(object);
  }

//  @Inline
//  public boolean isAfterTAMS(ObjectReference object) {
//    Address region = Region.of(object);
//    Address TAMS = Region.metaDataOf(region, Region.METADATA_TAMS_OFFSET).loadAddress();
//    return VM.objectModel.objectStartRef(object).GE(TAMS);
//  }

//  @Inline
//  public float heapWastePercent(boolean oldGenerationOnly) {
//    int usedSize = 0;
//    int totalRegions = 0;
//    Address region = firstRegion();
//    while (!region.isZero()) {
//      if (!oldGenerationOnly || Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt() == Region.OLD) {
//        usedSize += Region.usedSize(region);
//        totalRegions++;
//      }
//      region = nextRegion(region);
//    }
//    if (totalRegions == 0) return 0;
//    return (1f - usedSize / (totalRegions * Region.BYTES_IN_REGION)) * 100;
//  }

  // Region iterator

//  @Inline
//  public Address firstRegion() {
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!contiguous);
//    return nextRegion(headDiscontiguousRegion);
//  }

//  @Inline
//  public Address nextRegion(Address region) {
//    Address chunk = EmbeddedMetaData.getMetaDataBase(region);
//    int i = region.EQ(chunk) ? 0 : Region.indexOf(region) + 1;
//    if (i >= Region.REGIONS_IN_CHUNK) {
//      i = 0;
//      chunk = getNextChunk(chunk);
//    }
////HeapLayout.vmMap.getContiguousRegionChunks()
//    while (true) {
//      if (chunk.isZero() || !Space.isMappedAddress(chunk)) return Address.zero();
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Space.isMappedAddress(chunk));
//      // Get allocated state of the next region in chunk
//      Address allocated = chunk.plus(Region.METADATA_OFFSET_IN_CHUNK + i * Region.METADATA_BYTES + Region.METADATA_ALLOCATED_OFFSET);
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Space.isMappedAddress(allocated));
//      if (allocated.loadByte() != ((byte) 0)) {
//        return chunk.plus(Region.REGIONS_START_OFFSET + i * Region.BYTES_IN_REGION);
//      }
//
//      i++;
//      if (i >= Region.REGIONS_IN_CHUNK) {
//        i = 0;
//        chunk = getNextChunk(chunk);
//      }
//    }
//  }

//  @Inline
//  private Address getNextChunk(Address chunk) {
//    return HeapLayout.vmMap.getNextContiguousRegion(chunk);
//  }

//  final Lock regionsLock = VM.newLock("regions-lock");
//  final byte[] regions = new byte[VM.HEAP_END.diff(VM.HEAP_START).toInt() >>> Region.LOG_BYTES_IN_REGION];
  final AddressArray regions = AddressArray.create(VM.HEAP_END.diff(VM.HEAP_START).toInt() >>> Region.LOG_BYTES_IN_REGION);
  @Inline
  private void insertRegion(Address region) {
//    regionsLock.acquire();
//    Address ptr = ObjectReference.fromObject(regions).toAddress();
//    int index = region.diff(VM.HEAP_START).toInt() >> Region.LOG_BYTES_IN_REGION;
//    ptr.plus(index).store(1);
    final int index = region.diff(VM.HEAP_START).toInt() >>> Region.LOG_BYTES_IN_REGION;
    if (VM.VERIFY_ASSERTIONS) {
      if (index < 0 || index >= regions.length()) {
        Log.writeln("invalid region: ", region);
        VM.assertions.fail("ArrayIndexOutOfBoundsException");
      }
    }
    regions.set(index, region);
//    regionsLock.release();
  }
  @Inline
  private void removeRegion(Address region) {
//    Address ptr = ObjectReference.fromObject(regions).toAddress();
//    int index = region.diff(VM.HEAP_START).toInt() >> Region.LOG_BYTES_IN_REGION;
//    ptr.plus(index).store(0);
//    regionsLock.acquire();
    regions.set(region.diff(VM.HEAP_START).toInt() >>> Region.LOG_BYTES_IN_REGION, Address.zero());
//    regionsLock.release();
  }

  int cursor = 0;

  @Inline
  public void resetRegionIterator() {
    cursor = 0;
  }

  @Inline
  public Address nextRegion() {
//      Address ptr = ObjectReference.fromObject(regions).toAddress();
    final int len = regions.length();
    while (cursor < len) {
      Address region = regions.get(cursor);
      cursor += 1;
      if (!region.isZero()) {
        return region; //VM.HEAP_START.plus(cursor << Region.LOG_BYTES_IN_REGION);
      }
    }
    return Address.zero();


//      if (region.isZero()) return Address.zero();
//      // move `region` to next allocated G1 region
//      while (!region.isZero() && (EmbeddedMetaData.getMetaDataBase(region).EQ(region) || !Region.allocated(region))) {
//        region = region.plus(Region.BYTES_IN_REGION);
//        if (EmbeddedMetaData.getMetaDataBase(region).EQ(region)) {
//          // Bump to next chunk
//          chunkCursor += 1;
//          region = chunk = chunk.plus(EmbeddedMetaData.BYTES_IN_REGION);
//          if (chunkCursor >= chunkLimit) {
//            // next contiguous region
//            region = chunk = contiguousRegion = HeapLayout.vmMap.getNextContiguousRegion(contiguousRegion);
//            chunkCursor = 0;
//            chunkLimit = contiguousRegion.isZero() ? 0 : HeapLayout.vmMap.getContiguousRegionChunks(contiguousRegion);
//          }
//        }
//      }
//      if (VM.VERIFY_ASSERTIONS) {
//        VM.assertions._assert(region.isZero() || Space.isMappedAddress(region));
//        VM.assertions._assert(region.isZero() || Space.isInSpace(getDescriptor(), region));
//        VM.assertions._assert(region.isZero() || Region.allocated(region));
//      }
//      Address result = region;
//      region = region.plus(Region.BYTES_IN_REGION);
//      return result;

//      Address result = region;
//      // Skip first metadata region
//      if (EmbeddedMetaData.getMetaDataBase(region).EQ(region)) {
//        result = region = region.plus(Region.BYTES_IN_REGION);
//      }
//
//      // update next region
//      region = region.plus(Region.BYTES_IN_REGION);
//      if (EmbeddedMetaData.getMetaDataBase(region).EQ(region) || Region.indexOf(region) >= Region.REGIONS_IN_CHUNK) {
//        chunkCursor += 1;
//        region = chunk = chunk.plus(EmbeddedMetaData.BYTES_IN_REGION);
//        if (chunkCursor >= chunkLimit) {
//          region = chunk = contiguousRegion = HeapLayout.vmMap.getNextContiguousRegion(contiguousRegion);
//          chunkCursor = 0;
//          chunkLimit = contiguousRegion.isZero() ? 0 : HeapLayout.vmMap.getContiguousRegionChunks(contiguousRegion);
//        }
//      }
//      return result;
  }

  @Uninterruptible
  public class RegionIterator {
    @Inline
    public void reset() {
      resetRegionIterator();
    }

    @Inline
    public Address next() {
      return nextRegion();
    }
  }

  public final RegionIterator regionIterator = new RegionIterator();

//  @NoInline
//  public int calculateRemSetPages() {
//      int remsetPages = 0;
//      int fixedPages = 0;
//      regionIterator.reset();
//      Address region;
//      while (!(region = regionIterator.next()).isZero()) {
//        fixedPages += 1;
//        remsetPages += Region.metaDataOf(region, Region.METADATA_REMSET_PAGES_OFFSET).loadInt();
////          remsetCards += Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
//      }
//      return (remsetPages / 16) + fixedPages;
//  }

//  @NoInline
//  public int calculateRemSetCards() {
//    int remsetCards = 0;
//    regionIterator.reset();
//    Address region;
//    while (!(region = regionIterator.next()).isZero()) {
//      if (Region.allocated(region))
//        remsetCards += Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
//    }
//    return remsetCards;
//  }

//  @Inline
  public AddressArray snapshotRegions(boolean nurseryOnly) {
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Plan.gcInProgress());

    int regionsCount = nurseryOnly ? youngRegions() : committedRegions();
    AddressArray regions = AddressArray.create(regionsCount);

    // Initialize regions array
    int index = 0;
    regionIterator.reset();
    Address region = regionIterator.next();
    while (!region.isZero()) {
      if (!nurseryOnly || Region.getInt(region, Region.MD_GENERATION) != Region.OLD) {
        regions.set(index, region);
        index++;
      }
      if (index >= regions.length()) break;
      region = regionIterator.next();//(region);
    }

    return regions;
  }

  /** Include all nursery regions and some old regions */
  @Inline
  public static AddressArray computeRelocationRegions(AddressArray regions, boolean includeAllNursery, boolean concurrent) {
    // Perform relocation set selection
    int regionsCount = regions.length();
    // Initialize regionsSizes array
    AddressArray regionsSizes = AddressArray.create(regionsCount);
    for (int i = 0; i < regionsCount; i++) {
      Address region = regions.get(i);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!region.isZero());
      if (!region.isZero()) {
        int size = Region.usedSize(region);
//        size += delta;
        regionsSizes.set(i, Address.fromIntZeroExtend(size));
      } else {
        regionsSizes.set(i, Address.zero());
      }
    }

    // quick sort
    AddressArrayQuickSort.sort(regions, regionsSizes);

    // select relocation regions
//    final int BOOT_PAGES = VM.AVAILABLE_START.diff(VM.HEAP_START).toInt() / Constants.BYTES_IN_PAGE;
    int availPages = VM.activePlan.global().getPagesAvail();// - BOOT_PAGES;
    int availRegions = ((int) (availPages / EmbeddedMetaData.PAGES_IN_REGION)) * Region.REGIONS_IN_CHUNK;
    int usableBytesForCopying = (int) (availRegions << Region.LOG_BYTES_IN_REGION);

    int currentSize = 0;
    int relocationRegions = 0;

    // Include all nursery regions
    if (includeAllNursery) {
      for (int i = 0; i < regions.length(); i++) {
        int size = regionsSizes.get(i).toInt();
        Address region = regions.get(i);
        if (Region.getInt(region, Region.MD_GENERATION) != Region.OLD) {
          // This is a nursery region
          if (currentSize + size >= usableBytesForCopying) {
            regions.set(i, Address.zero());
            continue;
          }
          currentSize += size;
          relocationRegions++;
        }
      }
    }
    // Include some old regions
    for (int i = 0; i < regions.length(); i++) {
      int size = regionsSizes.get(i).toInt();
      Address region = regions.get(i);
      if (region.isZero()) continue;
      if (includeAllNursery && Region.getInt(region, Region.MD_GENERATION) != Region.OLD) {
        // This region is already included
        continue;
      } else if (currentSize + size >= usableBytesForCopying || size > Region.BYTES_IN_REGION * 0.65) {
        regions.set(i, Address.zero());
        continue;
      }
      currentSize += size;
      relocationRegions++;
    }

    // Return relocationSet array
    AddressArray relocationSet = AddressArray.create(relocationRegions);
    int cursor = 0;
    for (int i = 0; i < regions.length(); i++) {
      Address region = regions.get(i);
      if (!region.isZero()) {
//        if (VM.VERIFY_ASSERTIONS) Log.writeln("Relocate ", region);
        relocationSet.set(cursor++, region);
      }
    }
//    if (VM.VERIFY_ASSERTIONS) Log.writeln("Relocation set size = ", cursor);

    return relocationSet;
  }

  @Inline
  public static void markRegionsAsRelocate(AddressArray regions) {
    for (int i = 0; i < regions.length(); i++) {
      Address region = regions.get(i);
      if (!region.isZero()) {
        Region.set(region, Region.MD_RELOCATE, true);
      }
    }
  }

  @Inline
  public boolean contains(Address address) {
    return !address.isZero() && Space.isInSpace(descriptor, address) && Region.getBool(Region.of(address), Region.MD_ALLOCATED);
  }

  @Inline
  public boolean contains(ObjectReference ref) {
    return contains(VM.objectModel.refToAddress(ref));
  }

  @Inline
  private static int ceilDiv(int x, int y) {
    return (x + y - 1) / y;
  }

  @Inline
  public void cleanupRegions(AddressArray relocationSet, boolean concurrent) {
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().rendezvous();
    int regionsToRelease = ceilDiv(relocationSet.length(), workers);

    for (int i = 0; i < regionsToRelease; i++) {
      int cursor = regionsToRelease * id + i;
      if (cursor >= relocationSet.length()) break;
      Address region = relocationSet.get(cursor);
      relocationSet.set(cursor, Address.zero());
      if (!region.isZero()) {
//        if (Region.verbose()) Log.writeln("Release ", region);
        this.release(region);
      }
    }
    VM.activePlan.collector().rendezvous();
  }

  /**
   * Heap Validation Methods
   */

//  public void validate() {
//    VM.assertions.fail("Unimplemented");
//    for (Address region = firstRegion(); !region.isZero(); region = nextRegion(region)) {
//      Region.linearScan(regionValidationLinearScan, region);
//    }
//  }

}
